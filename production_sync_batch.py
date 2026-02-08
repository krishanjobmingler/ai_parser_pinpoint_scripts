#!/usr/bin/env python3

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from bson import ObjectId

BATCH_SIZE = 1000
MAX_WORKERS = 4

LOCAL_URI = "mongodb://localhost:27017"
LOCAL_DB = "jobminglr_staging"

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_BASE_DIR = os.path.join(SCRIPT_DIR, "logs")
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOGS_BASE_DIR, TODAY_DATE)
LOG_FILE = os.path.join(LOG_DIR, "production_sync.log")

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("production_sync")
logger.setLevel(logging.DEBUG)
logger.propagate = False

console_formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
file_formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

try:
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    print(f"Log file: {LOG_FILE}")
except Exception as e:
    print(f"Could not create log file: {e}")


def log(m):
    logger.info(m)


def log_debug(m):
    logger.debug(m)


def prepare_job_for_sync(job: dict) -> tuple:
    job_id = job.get("_id")
    source_key = job.get("sourceKey")
    absolute_url = job.get("absoluteUrl")

    job_doc = job.copy()
    job_doc.pop("_id", None)
    job_doc.pop("validated", None)
    job_doc.pop("synced", None)
    job_doc.pop("syncedAt", None)
    job_doc.pop("sourceKey", None)

    if absolute_url:
        lookup_filter = {"absoluteUrl": absolute_url}
    else:
        lookup_filter = {"company": job_doc.get("company"), "jobTitleForFrontend": job_doc.get("jobTitleForFrontend")}

    return job_id, source_key, lookup_filter, job_doc


def process_batch(batch_jobs, batch_num, total_batches, jobs_prod_col, jobs_ready_col):
    batch_start = datetime.now(timezone.utc)

    prod_operations = []
    local_update_ids = []
    job_details = []

    for job in batch_jobs:
        job_id, source_key, lookup_filter, job_doc = prepare_job_for_sync(job)

        prod_operations.append(
            UpdateOne(
                lookup_filter,
                {"$set": job_doc, "$unset": {"sourceKey": "", "validated": "", "synced": "", "syncedAt": ""}},
                upsert=True
            )
        )

        local_update_ids.append(job_id)
        job_details.append({"source_key": source_key, "title": job_doc.get("jobTitleForFrontend", "")[:40]})

    synced_count = 0
    if prod_operations:
        try:
            result = jobs_prod_col.bulk_write(prod_operations, ordered=False)
            synced_count = result.modified_count + result.upserted_count
            for detail in job_details:
                log_debug(f"SYNCED: {detail['source_key']} | title={detail['title']}")
        except Exception as e:
            log(f"Batch {batch_num} production bulk write error: {e}")
            return {"batch": batch_num, "synced": 0, "errors": len(batch_jobs), "duration": 0}

    if local_update_ids:
        try:
            jobs_ready_col.update_many(
                {"_id": {"$in": local_update_ids}},
                {"$set": {"synced": True, "syncedAt": datetime.now(timezone.utc)}}
            )
        except Exception as e:
            log(f"Batch {batch_num} local update error: {e}")

    batch_duration = (datetime.now(timezone.utc) - batch_start).total_seconds()
    log(f"Batch {batch_num}/{total_batches} — {synced_count} jobs synced ({batch_duration:.2f}s)")

    return {"batch": batch_num, "synced": synced_count, "errors": 0, "duration": batch_duration}


def main():
    start_time = datetime.now(timezone.utc)

    client_local = MongoClient(LOCAL_URI)
    db_local = client_local[LOCAL_DB]

    client_prod = MongoClient(PROD_URI)
    db_prod = client_prod[PROD_DB]

    jobs_ready = db_local["jobs"]
    jobs_prod = db_prod["jobs"]

    log("Connected:")
    log(f" - Local: {LOCAL_URI}")
    log(f" - Production: {PROD_URI.replace('resume_builder:itOceb9dM0wKN3uE', '********')}")
    log(f" - Batch size: {BATCH_SIZE} | Max workers: {MAX_WORKERS}")

    ready_jobs = list(jobs_ready.find({"validated": True, "synced": {"$ne": True}}))
    total_count = len(ready_jobs)

    log(f"Syncing {total_count} validated jobs to production...")

    if total_count == 0:
        log("No jobs ready for sync.")
        client_local.close()
        client_prod.close()
        return

    batches = [ready_jobs[i:i + BATCH_SIZE] for i in range(0, total_count, BATCH_SIZE)]
    total_batches = len(batches)

    log(f"Processing {total_batches} batches...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_batch, batch, idx + 1, total_batches, jobs_prod, jobs_ready): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                batch_idx = futures[future]
                log(f"Batch {batch_idx + 1} failed: {e}")
                results.append({"batch": batch_idx + 1, "synced": 0, "errors": BATCH_SIZE, "duration": 0})

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()

    total_synced = sum(r["synced"] for r in results)
    total_errors = sum(r["errors"] for r in results)

    log("-" * 60)
    log("SUMMARY")
    log("-" * 60)
    log(f"Total jobs ready: {total_count}")
    log(f"Jobs synced: {total_synced}")
    log(f"Errors: {total_errors}")
    log(f"Duration: {duration:.2f}s")
    log(f"Rate: {total_synced/duration:.2f} jobs/sec" if duration > 0 else "Rate: N/A")
    log("-" * 60)
    log(f"Log: {LOG_FILE}")

    log_debug("=" * 60)
    summary_json = {
        "run_date": TODAY_DATE,
        "duration_seconds": round(duration, 2),
        "total_jobs_ready": total_count,
        "jobs_synced": total_synced,
        "errors": total_errors,
        "batches_processed": total_batches,
        "rate_jobs_per_sec": round(total_synced / duration, 2) if duration > 0 else 0,
    }
    log_debug(json.dumps(summary_json, indent=2))
    log_debug("=" * 60)

    client_local.close()
    client_prod.close()


if __name__ == "__main__":
    main()
