#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
import aiohttp
import psutil

from new_extraction import SkillExtractor

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "jobminglr_staging")
DEBUG_COLLECTION = "ai_parsing_debug"
SKILLS_COLLECTION = "skills"

ENRICH_BATCH_URL = os.getenv("ENRICH_BATCH_URL", "http://localhost:8001/enrich/batch")
VECTOR_THRESHOLD = float(os.getenv("ENRICH_VECTOR_THRESHOLD", "0.70"))
ENABLE_VECTOR_SEARCH = os.getenv("ENABLE_VECTOR_SEARCH", "false").lower() == "true"
USE_LOCAL_SKILL_MATCHING = os.getenv("USE_LOCAL_SKILL_MATCHING", "true").lower() == "true"
ENRICH_TIMEOUT = int(os.getenv("ENRICH_TIMEOUT", "0"))
ENRICH_RETRIES = int(os.getenv("ENRICH_RETRIES", "3"))
ENRICH_RETRY_BACKOFF = float(os.getenv("ENRICH_RETRY_BACKOFF", "2.0"))
BATCH_SIZE = int(os.getenv("ENRICH_BATCH_SIZE", "500"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_BASE_DIR = os.path.join(SCRIPT_DIR, "logs")
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOGS_BASE_DIR, TODAY_DATE)
LOG_FILE = os.path.join(LOG_DIR, "ai_parsing.log")

os.makedirs(LOG_DIR, exist_ok=True)

log = logging.getLogger("pinpoint_parse_ai")
log.setLevel(logging.DEBUG)
log.propagate = False

console_formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
file_formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)
log.addHandler(console_handler)

try:
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    log.addHandler(file_handler)
    print(f"Log file: {LOG_FILE}")
except Exception as e:
    print(f"Could not create log file: {e}")


def log_job_detail(job_id, title, skills_count, quals_count, status, details=None):
    detail_str = ""
    if details:
        detail_str = " | " + " | ".join(f"{k}={v}" for k, v in details.items())
    log.debug(f"JOB: {job_id} | title='{title[:50]}' | skills={skills_count} | quals={quals_count} | status={status}{detail_str}")


class ResourceMonitor:
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.cpu_samples = []
        self.ram_samples = []
        self.peak_ram_mb = 0
        self.start_ram_mb = 0
        self.monitoring = False
        self._task = None

    def get_system_info(self):
        mem = psutil.virtual_memory()
        return {
            "total_ram_gb": round(mem.total / (1024 ** 3), 2),
            "available_ram_gb": round(mem.available / (1024 ** 3), 2),
            "ram_percent": mem.percent,
            "cpu_count": psutil.cpu_count(),
            "cpu_percent": psutil.cpu_percent(interval=0.1)
        }

    def get_process_info(self):
        try:
            mem_info = self.process.memory_info()
            return {
                "ram_mb": round(mem_info.rss / (1024 ** 2), 2),
                "ram_percent": round(mem_info.rss / psutil.virtual_memory().total * 100, 2),
                "cpu_percent": self.process.cpu_percent(interval=0.1)
            }
        except Exception:
            return {"ram_mb": 0, "ram_percent": 0, "cpu_percent": 0}

    async def _monitor_loop(self, interval=1.0):
        while self.monitoring:
            try:
                info = self.get_process_info()
                self.cpu_samples.append(info["cpu_percent"])
                self.ram_samples.append(info["ram_mb"])
                if info["ram_mb"] > self.peak_ram_mb:
                    self.peak_ram_mb = info["ram_mb"]
            except Exception:
                pass
            await asyncio.sleep(interval)

    async def start(self):
        self.start_ram_mb = self.get_process_info()["ram_mb"]
        self.monitoring = True
        self._task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        self.monitoring = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def get_summary(self):
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        avg_ram = sum(self.ram_samples) / len(self.ram_samples) if self.ram_samples else 0
        max_cpu = max(self.cpu_samples) if self.cpu_samples else 0
        min_ram = min(self.ram_samples) if self.ram_samples else 0
        return {
            "start_ram_mb": self.start_ram_mb,
            "peak_ram_mb": self.peak_ram_mb,
            "avg_ram_mb": round(avg_ram, 2),
            "min_ram_mb": round(min_ram, 2),
            "avg_cpu_percent": round(avg_cpu, 2),
            "max_cpu_percent": round(max_cpu, 2),
            "samples_count": len(self.cpu_samples)
        }


def format_duration(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {seconds % 60:.2f}s"
    else:
        return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m {seconds % 60:.2f}s"


def format_bytes(mb):
    if mb < 1024:
        return f"{mb:.2f} MB"
    return f"{mb / 1024:.2f} GB"


def strip_html(html):
    return BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)


skill_matcher = SkillExtractor()


def prepare_job_for_batch(job):
    raw = job.get("rawSections") or {}
    desc_html = raw.get("description") or job.get("jobDescription") or ""
    resp_html = raw.get("key_responsibilities") or job.get("keyResponsibilities") or ""
    skills_html = raw.get("skills_knowledge_expertise") or job.get("skillsKnowledgeExpertise") or ""
    benefits_html = raw.get("benefits") or job.get("benefits") or ""

    desc_text = strip_html(desc_html)
    skills_text = strip_html(skills_html)

    combined_html = f"{desc_html}<hr>{resp_html}<hr>{skills_html}<hr>{benefits_html}"
    cleaned = strip_html(combined_html)

    return {
        "id": job.get("sourceKey"),
        "title": job.get("jobTitleForFrontend") or job.get("jobTitle") or "",
        "description": desc_text or cleaned,
        "skills_knowledge_expertise": skills_text or desc_text or "",
        "_cleaned": cleaned,
        "_job": job,
    }


def parse_batch_result(result):
    qualification_ids = []
    for qual in result.get("qualifications", []):
        if isinstance(qual, dict) and qual.get("id"):
            qualification_ids.append(qual["id"])
    return qualification_ids


async def save_debug_data(debug_collection, batch_index, api_input, api_output, prepared_jobs):
    try:
        debug_doc = {
            "batch_index": batch_index,
            "timestamp": datetime.now(timezone.utc),
            "date": TODAY_DATE,
            "jobs_count": len(prepared_jobs),
            "api_input": {
                "url": ENRICH_BATCH_URL,
                "jobs": [
                    {
                        "id": j["id"],
                        "title": j["title"],
                        "description": j["description"][:500] + "..." if len(j.get("description", "")) > 500 else j.get("description", ""),
                    }
                    for j in prepared_jobs
                ],
                "enable_vector_search": ENABLE_VECTOR_SEARCH,
                "vector_threshold": VECTOR_THRESHOLD,
            },
            "api_output": {
                "status": api_output.get("status"),
                "processing_time_ms": api_output.get("processing_time_ms", 0),
                "successful": api_output.get("successful", 0),
                "failed": api_output.get("failed", 0),
                "results_count": len(api_output.get("results", {})),
                "results": {
                    job_id: {
                        "skills_count": len(result.get("skills", [])),
                        "qualifications_count": len(result.get("qualifications", [])),
                        "qualifications": result.get("qualifications", [])[:5],
                    }
                    for job_id, result in list(api_output.get("results", {}).items())[:20]
                }
            }
        }
        await debug_collection.insert_one(debug_doc)
        log.debug("[Batch %d] Debug data saved", batch_index)
    except Exception as e:
        log.warning("[Batch %d] Failed to save debug data: %s", batch_index, e)


async def enrich_batch(session, jobs_batch):
    payload = {
        "jobs": [
            {
                "id": j["id"],
                "title": j["title"],
                "description": j["description"],
                "skills_knowledge_expertise": j["skills_knowledge_expertise"],
            }
            for j in jobs_batch
        ],
        "enable_vector_search": True,
        "vector_threshold": VECTOR_THRESHOLD,
    }

    for attempt in range(1, ENRICH_RETRIES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=None) if ENRICH_TIMEOUT == 0 else aiohttp.ClientTimeout(total=ENRICH_TIMEOUT)
            async with session.post(ENRICH_BATCH_URL, json=payload, timeout=timeout) as resp:
                resp.raise_for_status()
                data = await resp.json() or {}

            results_map = {}
            for result in data.get("results", []):
                job_id = result.get("id")
                if job_id:
                    results_map[job_id] = result

            return {
                "status": "success",
                "results": results_map,
                "processing_time_ms": data.get("processing_time_ms", 0),
                "successful": data.get("successful", 0),
                "failed": data.get("failed", 0),
            }
        except Exception as exc:
            log.warning("Batch API failed attempt=%s/%s; error=%s: %s", attempt, ENRICH_RETRIES, type(exc).__name__, exc)
            if attempt < ENRICH_RETRIES:
                await asyncio.sleep(ENRICH_RETRY_BACKOFF)

    return {"status": "failed", "results": {}, "processing_time_ms": 0}


async def process_batch(jobs_batch, session, queue_collection, debug_collection, batch_index, total_batches, start_job_index, total_jobs):
    batch_start = datetime.now(timezone.utc)
    batch_size = len(jobs_batch)

    log.info("[Batch %d/%d] Processing %d jobs (jobs %d-%d of %d)...",
             batch_index, total_batches, batch_size, start_job_index + 1, start_job_index + batch_size, total_jobs)

    prepared_jobs = []
    skipped_empty = 0

    for job in jobs_batch:
        prepared = prepare_job_for_batch(job)
        if not prepared["_cleaned"].strip():
            skipped_empty += 1
            log_job_detail(prepared["id"], prepared["title"], 0, 0, "SKIPPED", {"reason": "empty_content"})
            continue
        prepared_jobs.append(prepared)

    if not prepared_jobs:
        log.warning("[Batch %d/%d] All jobs empty, skipping batch", batch_index, total_batches)
        return {"status": "skipped", "success_count": 0, "error_count": 0, "skipped_count": skipped_empty, "skills_count": 0, "qualifications_count": 0, "duration": 0}

    api_result = await enrich_batch(session, prepared_jobs)
    await save_debug_data(debug_collection, batch_index, {}, api_result, prepared_jobs)

    if api_result["status"] == "failed":
        log.error("[Batch %d/%d] Batch API failed completely", batch_index, total_batches)
        return {"status": "error", "success_count": 0, "error_count": len(prepared_jobs), "skipped_count": skipped_empty, "skills_count": 0, "qualifications_count": 0, "duration": (datetime.now(timezone.utc) - batch_start).total_seconds()}

    results_map = api_result["results"]
    success_count = 0
    error_count = 0
    total_skills = 0
    total_qualifications = 0
    now = datetime.now(timezone.utc)

    local_skills_map = {}
    if USE_LOCAL_SKILL_MATCHING and skill_matcher.loaded:
        all_texts = [p["_cleaned"] for p in prepared_jobs]
        bulk_start = datetime.now(timezone.utc)
        local_skills_map = skill_matcher.match_skills_bulk(all_texts)
        bulk_duration = (datetime.now(timezone.utc) - bulk_start).total_seconds() * 1000
        total_local_skills = sum(len(skills) for skills in local_skills_map.values())
        log.debug("[Batch %d] BULK matching: %d jobs -> %d skills in %.2fms", batch_index, len(all_texts), total_local_skills, bulk_duration)

    bulk_updates = []

    for idx, prepared in enumerate(prepared_jobs):
        source_key = prepared["id"]
        job = prepared["_job"]
        cleaned = prepared["_cleaned"]

        result = results_map.get(source_key, {})
        qualification_ids = parse_batch_result(result)

        skills = []
        if USE_LOCAL_SKILL_MATCHING and idx in local_skills_map:
            local_skills = local_skills_map[idx]
            if local_skills:
                seen_ids = set()
                for ls in local_skills:
                    sid = ls.get("skill_id")
                    if sid and sid not in seen_ids:
                        seen_ids.add(sid)
                        skills.append(ls)

        qualification_ids = list(dict.fromkeys(qualification_ids))

        ai_result = {
            "skills": skills,
            "qualificationIds": qualification_ids,
            "summary": cleaned[:4000],
            "parsedAt": now
        }

        bulk_updates.append({
            "filter": {"sourceKey": source_key},
            "update": {"$set": {
                "companyId": job.get("companyId"),
                "jobTitleForFrontend": job.get("jobTitleForFrontend"),
                "employmentType": job.get("employmentType"),
                "workplaceType": job.get("workplaceType"),
                "location": job.get("location"),
                "jobType": job.get("jobType", "pinpoint"),
                "parsed": True,
                "ai_parsed": ai_result,
                "qualificationIds": qualification_ids,
                "validated": True,
                "parsedAt": now
            }}
        })

        log_job_detail(source_key, job.get("jobTitleForFrontend", ""), len(skills), len(qualification_ids), "SUCCESS",
            {"company": job.get("companyName", "")[:30], "location": (job.get("location") or "")[:30], "top_skills": [s.get("name", "")[:30] for s in skills[:5]]})

        success_count += 1
        total_skills += len(skills)
        total_qualifications += len(qualification_ids)

    if bulk_updates:
        for update in bulk_updates:
            await queue_collection.update_one(update["filter"], update["update"], upsert=True)

    batch_duration = (datetime.now(timezone.utc) - batch_start).total_seconds()
    api_time = api_result.get("processing_time_ms", 0) / 1000

    log.info("[Batch %d/%d] Done | success=%d | skipped=%d | skills=%d | quals=%d | API=%.2fs | total=%.2fs",
             batch_index, total_batches, success_count, skipped_empty, total_skills, total_qualifications, api_time, batch_duration)

    return {
        "status": "success", "success_count": success_count, "error_count": error_count,
        "skipped_count": skipped_empty, "skills_count": total_skills,
        "qualifications_count": total_qualifications, "duration": batch_duration, "api_time": api_time
    }


async def main():
    pipeline_start = datetime.now(timezone.utc)
    monitor = ResourceMonitor()
    sys_info = monitor.get_system_info()
    proc_info = monitor.get_process_info()

    log.info("=" * 80)
    log.info("PINPOINT AI PARSER - BATCH MODE")
    log.info("=" * 80)
    log.info("Start Time     : %s", pipeline_start.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("Batch Size     : %d", BATCH_SIZE)
    log.info("Enrich URL     : %s", ENRICH_BATCH_URL)
    log.info("Database       : %s", DB_NAME)
    log.info("RAM: %.2f GB | CPU: %d cores", sys_info["total_ram_gb"], sys_info["cpu_count"])
    log.info("-" * 80)

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    fetch_start = datetime.now(timezone.utc)
    cursor = db["pinpoint_jobs_raw"].find({"parsed": {"$ne": True}})
    jobs = await cursor.to_list(length=None)
    fetch_duration = (datetime.now(timezone.utc) - fetch_start).total_seconds()

    if not jobs:
        log.warning("No jobs found to parse.")
        return

    total_jobs = len(jobs)
    total_batches = (total_jobs + BATCH_SIZE - 1) // BATCH_SIZE

    log.info("Jobs: %d | Batches: %d | Fetch: %s", total_jobs, total_batches, format_duration(fetch_duration))

    await monitor.start()

    queue_collection = db["pinpoint_jobs_queue"]
    debug_collection = db[DEBUG_COLLECTION]

    if USE_LOCAL_SKILL_MATCHING:
        await skill_matcher.load_async(db, SKILLS_COLLECTION)
        log.info("Skill extractor loaded from '%s' (name + aliases)", SKILLS_COLLECTION)

    connector = aiohttp.TCPConnector(limit=10, limit_per_host=10)
    process_start = datetime.now(timezone.utc)
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        for batch_idx in range(total_batches):
            start_idx = batch_idx * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_jobs)
            batch_jobs = jobs[start_idx:end_idx]
            result = await process_batch(batch_jobs, session, queue_collection, debug_collection, batch_idx + 1, total_batches, start_idx, total_jobs)
            results.append(result)

    process_end = datetime.now(timezone.utc)
    await monitor.stop()
    resource_summary = monitor.get_summary()

    success_count = sum(r.get("success_count", 0) for r in results)
    skipped_count = sum(r.get("skipped_count", 0) for r in results)
    error_count = sum(r.get("error_count", 0) for r in results)
    total_skills = sum(r.get("skills_count", 0) for r in results)
    total_qualifications = sum(r.get("qualifications_count", 0) for r in results)
    total_api_time = sum(r.get("api_time", 0) for r in results)

    batch_durations = [r.get("duration", 0) for r in results if r.get("duration")]
    avg_batch_duration = sum(batch_durations) / len(batch_durations) if batch_durations else 0

    process_duration = (process_end - process_start).total_seconds()
    pipeline_end = datetime.now(timezone.utc)
    total_duration = (pipeline_end - pipeline_start).total_seconds()
    rate = total_jobs / process_duration if process_duration > 0 else 0

    final_proc_info = monitor.get_process_info()

    log.info("-" * 80)
    log.info("SUMMARY")
    log.info("-" * 80)
    log.info("Jobs: %d | Success: %d | Skipped: %d | Errors: %d", total_jobs, success_count, skipped_count, error_count)
    log.info("Skills: %d | Qualifications: %d", total_skills, total_qualifications)
    log.info("Duration: %s | API: %s | Rate: %.2f jobs/sec", format_duration(total_duration), format_duration(total_api_time), rate)
    log.info("RAM: start=%s peak=%s | CPU: avg=%.1f%%", format_bytes(resource_summary["start_ram_mb"]), format_bytes(resource_summary["peak_ram_mb"]), resource_summary["avg_cpu_percent"])
    log.info("=" * 80)
    log.info("Log: %s", LOG_FILE)

    summary_json = {
        "run_date": TODAY_DATE,
        "total_jobs": total_jobs,
        "success_count": success_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "total_skills": total_skills,
        "total_qualifications": total_qualifications,
        "duration_seconds": round(total_duration, 2),
        "throughput": round(rate, 2),
        "peak_ram_mb": resource_summary["peak_ram_mb"],
    }
    log.debug(json.dumps(summary_json, indent=2))

    client.close()

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
