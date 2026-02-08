#!/usr/bin/env python3

import html
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from rapidfuzz import fuzz, process

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "jobminglr_staging"
PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_BASE_DIR = os.path.join(SCRIPT_DIR, "logs")
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOGS_BASE_DIR, TODAY_DATE)
LOG_FILE = os.path.join(LOG_DIR, "canonical_mapper.log")

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("canonical_mapper")
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


client = MongoClient(MONGO_URI)
db = client[DB_NAME]

client_prod = MongoClient(PROD_URI)
db_prod = client_prod[PROD_DB]
companies_prod = db_prod["companies"]
job_titles_prod = db_prod["job-titles"]
skills_prod = db_prod["skills"]

queue = db["pinpoint_jobs_queue"]
jobs = db["jobs"]
raw_jobs = db["pinpoint_jobs_raw"]

log("Starting Pinpoint canonical mapping...")
log(f"Connected to production: {PROD_DB}.companies, {PROD_DB}.job-titles, {PROD_DB}.skills")

def get_or_create_company(company_name: str) -> ObjectId:
    if not company_name:
        return None

    existing = companies_prod.find_one(
        {"name": {"$regex": f"^{re.escape(company_name)}$", "$options": "i"}},
        {"_id": 1}
    )

    if existing:
        return existing["_id"]

    now = datetime.now(timezone.utc)
    new_company = {
        "logo": "/splash.png",
        "name": company_name,
        "size": 0,
        "address": "N/A",
        "location": {"type": "Point", "coordinates": [0, 0]},
        "category": None,
        "websiteUrl": "N/A",
        "createdBy": "system",
        "updatedBy": "system",
        "createdAt": now,
        "updatedAt": now,
    }

    result = companies_prod.insert_one(new_company)
    log_debug(f"CREATE company: '{company_name}' -> {result.inserted_id}")
    return result.inserted_id

company_cache = {}
log("Loading companies from production...")
for c in companies_prod.find({}, {"_id": 1, "name": 1}):
    if c.get("name"):
        company_cache[c["name"].lower()] = c["_id"]
log(f"Loaded {len(company_cache)} companies")

job_title_lookup = {}
job_title_ids = {}

log("Loading job titles with aliases from production...")
for jt in job_titles_prod.find({"isDeleted": {"$ne": True}}, {"_id": 1, "title": 1, "aliases": 1}):
    jt_id = jt["_id"]
    title = jt.get("title", "").strip()
    aliases = jt.get("aliases", []) or []

    if title:
        job_title_lookup[title.lower()] = (jt_id, title)
        job_title_ids[jt_id] = title

    for alias in aliases:
        if alias and isinstance(alias, str):
            alias_clean = alias.strip()
            if alias_clean:
                job_title_lookup[alias_clean.lower()] = (jt_id, alias_clean)

log(f"Loaded {len(job_title_ids)} job titles with {len(job_title_lookup)} searchable terms")


def get_job_title_id(title: str) -> ObjectId:
    if not title:
        return None

    title_lower = title.strip().lower()

    if title_lower in job_title_lookup:
        jt_id, matched_text = job_title_lookup[title_lower]
        return jt_id

    all_terms = list(job_title_lookup.keys())
    if not all_terms:
        return None

    result = process.extractOne(title_lower, all_terms, scorer=fuzz.WRatio)

    if result:
        matched_term, score, _ = result
        jt_id, original_text = job_title_lookup[matched_term]
        log_debug(f"MATCH job_title: '{title}' -> '{original_text}' (score: {score})")
        return jt_id

    return None

skill_lookup = {}
skill_ids = set()
skill_fuzzy_cache = {}
pending_alias_updates = {}

log("Loading skills with aliases from production...")
for s in skills_prod.find({}, {"_id": 1, "name": 1, "aliases": 1}):
    skill_id = s["_id"]
    skill_ids.add(skill_id)

    name = s.get("name", "")
    if name:
        skill_lookup[name.strip().lower()] = (skill_id, name)

    aliases = s.get("aliases", [])
    for alias in aliases:
        if alias:
            alias_lower = alias.strip().lower()
            if alias_lower not in skill_lookup:
                skill_lookup[alias_lower] = (skill_id, alias)

skill_terms_list = list(skill_lookup.keys())
log(f"Loaded {len(skill_ids)} skills with {len(skill_lookup)} searchable terms")


def get_skill_id(skill_name: str) -> ObjectId:
    if not skill_name:
        return None

    skill_lower = skill_name.strip().lower()

    if skill_lower in skill_lookup:
        skill_id, matched_text = skill_lookup[skill_lower]
        return skill_id

    if skill_lower in skill_fuzzy_cache:
        skill_id, original_text, score = skill_fuzzy_cache[skill_lower]
        return skill_id

    if not skill_terms_list:
        return None

    result = process.extractOne(skill_lower, skill_terms_list, scorer=fuzz.WRatio)

    if result:
        matched_term, score, _ = result
        skill_id, original_text = skill_lookup[matched_term]
        skill_fuzzy_cache[skill_lower] = (skill_id, original_text, score)

        if score >= 70:
            if skill_id not in pending_alias_updates:
                pending_alias_updates[skill_id] = set()
            pending_alias_updates[skill_id].add(skill_lower)
            skill_lookup[skill_lower] = (skill_id, skill_name)

        return skill_id

    return None


def flush_pending_alias_updates():
    if not pending_alias_updates:
        return 0

    operations = []
    for skill_id, aliases in pending_alias_updates.items():
        operations.append(UpdateOne({"_id": skill_id}, {"$addToSet": {"aliases": {"$each": list(aliases)}}}))

    if operations:
        result = skills_prod.bulk_write(operations, ordered=False)
        count = len(operations)
        log_debug(f"BATCH alias update: {count} skills updated")
        pending_alias_updates.clear()
        return count

    return 0

def normalize_employment_type(emp_type):
    if not emp_type:
        return None
    emp_type = emp_type.strip().lower()
    if "full" in emp_type and "time" in emp_type:
        return "full-time"
    elif "part" in emp_type and "time" in emp_type:
        return "part-time"
    elif "contract" in emp_type:
        return "contract"
    elif "temporary" in emp_type or "temp" in emp_type:
        return "temporary"
    elif "intern" in emp_type:
        return "internship"
    return emp_type.replace(" ", "-")

def normalize_workplace_type(workplace):
    if not workplace:
        return None
    workplace_lower = workplace.strip().lower()
    if "onsite" in workplace_lower or "on-site" in workplace_lower or "office" in workplace_lower:
        return "In-Office"
    elif "remote" in workplace_lower:
        return "Remote"
    elif "hybrid" in workplace_lower:
        return "Hybrid"
    return workplace.strip()


def extract_rate(raw_job: dict) -> str:
    if not raw_job:
        return "year"

    freq = raw_job.get("compensationFrequency") or raw_job.get("compensation_frequency")
    if freq:
        freq_lower = freq.lower()
        if "year" in freq_lower or "annual" in freq_lower:
            return "year"
        elif "month" in freq_lower:
            return "month"
        elif "week" in freq_lower:
            return "week"
        elif "hour" in freq_lower:
            return "hour"
        elif "day" in freq_lower:
            return "day"
        return freq_lower

    comp = raw_job.get("compensation") or ""
    comp_lower = comp.lower()

    if "year" in comp_lower or "annual" in comp_lower:
        return "year"
    elif "month" in comp_lower:
        return "month"
    elif "week" in comp_lower:
        return "week"
    elif "hour" in comp_lower:
        return "hour"
    elif "day" in comp_lower:
        return "day"

    return "year"

def clean_html_entities(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_benefits(benefits):
    if not benefits:
        return []
    if isinstance(benefits, list):
        cleaned = []
        for b in benefits:
            if b:
                clean_b = clean_html_entities(str(b))
                if clean_b:
                    cleaned.append(clean_b)
        return cleaned
    if isinstance(benefits, str):
        clean_str = clean_html_entities(benefits)
        return [clean_str] if clean_str else []
    return []

parsed_jobs = list(queue.find({"parsed": True, "mapped": {"$ne": True}}))
log(f"Found {len(parsed_jobs)} parsed jobs to map")

start_time = datetime.now(timezone.utc)
mapped = 0
total_skills = 0
total_qualifications = 0

for job in parsed_jobs:
    log(f"Mapping job {job['_id']}")

    parsed = job.get("ai_parsed")
    if not parsed:
        log_debug(f"SKIP missing_ai_parsed: job_id={job['_id']}")
        continue

    source_key = job.get("sourceKey")
    raw_job = raw_jobs.find_one({"sourceKey": source_key}) if source_key else None

    company_name = raw_job.get("companyName") if raw_job else None
    if not company_name:
        log_debug(f"SKIP missing_company: job_id={job['_id']}")
        continue

    company_id = company_cache.get(company_name.lower())
    if not company_id:
        company_id = get_or_create_company(company_name)
        if company_id:
            company_cache[company_name.lower()] = company_id

    if not company_id:
        log_debug(f"SKIP company_failed: job_id={job['_id']} | company={company_name}")
        continue

    title_raw = (
        parsed.get("jobTitleCanonical")
        or parsed.get("jobTitle")
        or job.get("jobTitleForFrontend")
        or (raw_job.get("jobTitleForFrontend") if raw_job else None)
        or ""
    ).strip()

    if not title_raw:
        log_debug(f"SKIP empty_title: job_id={job['_id']}")
        continue

    title_id = get_job_title_id(title_raw)

    skill_required = []
    good_to_have_skills = []

    for s in parsed.get("skills", []):
        name = s.get("name", "").strip()
        if not name:
            continue

        skill_id = get_skill_id(name)

        if not skill_id:
            log_debug(f"SKIP skill_no_match: job_id={job['_id']} | skill={name}")
            continue

        weight = s.get("weight", 5)
        required = s.get("required", True)

        skill_obj = {"skillId": skill_id, "weight": weight, "_id": ObjectId()}

        if required:
            skill_required.append(skill_obj)
        else:
            good_to_have_skills.append(skill_obj)

    min_salary = 0
    max_salary = 0
    is_salary_disclosed = False

    if raw_job:
        min_salary = raw_job.get("compensationMinimum") or raw_job.get("compensation_minimum") or 0
        max_salary = raw_job.get("compensationMaximum") or raw_job.get("compensation_maximum") or 0
        min_salary = int(min_salary) if min_salary else 0
        max_salary = int(max_salary) if max_salary else 0
        is_salary_disclosed = (
            raw_job.get("isSalaryDisclosed", False) or raw_job.get("compensation_visible", False)
        ) and (min_salary > 0 or max_salary > 0)

    benefits_raw = raw_job.get("benefits") if raw_job else None
    benefits = parse_benefits(benefits_raw)

    location_data = raw_job.get("location") if raw_job else None
    if isinstance(location_data, dict):
        address = location_data.get("name", "")
    else:
        address = job.get("location") or (location_data if location_data else None)

    location_geojson = {"type": "Point", "coordinates": [0, 0]} if address else None

    absolute_url = raw_job.get("absoluteUrl") if raw_job else None

    pinpoint_id = None
    if absolute_url:
        url_parts = absolute_url.rstrip('/').split('/')
        if url_parts:
            last_part = url_parts[-1]
            match = re.match(r'^(\d+)', last_part)
            if match:
                pinpoint_id = match.group(1)

    employment_type_raw = job.get("employmentType") or (raw_job.get("employmentType") if raw_job else None)
    employment_type = normalize_employment_type(employment_type_raw)

    workplace_type_raw = job.get("workplaceType") or (raw_job.get("workplaceType") if raw_job else None)
    work_arrangement = normalize_workplace_type(workplace_type_raw)

    experience_level = job.get("experienceLevel") or "intermediate"
    rate = extract_rate(raw_job)

    qualification_ids_raw = job.get("qualificationIds") or parsed.get("qualificationIds") or []
    qualification_ids = []
    for qid in qualification_ids_raw:
        try:
            qualification_ids.append(ObjectId(qid) if isinstance(qid, str) else qid)
        except Exception:
            continue

    system_user_id = ObjectId("67ffb82f4ab56c8e9449ed48")

    now = datetime.now(timezone.utc)
    doc = {
        "absoluteUrl": absolute_url,
        "address": address,
        "benefits": benefits,
        "company": company_id,
        "companyCulture": [],
        "createdAt": now,
        "createdBy": system_user_id,
        "employmentType": employment_type,
        "experienceLevel": experience_level,
        "goodToHaveSkill": good_to_have_skills,
        "greenHouseId": None,
        "isActive": True,
        "isAddedFromGreenhouse": False,
        "isDeleted": False,
        "isSalaryDisclosed": is_salary_disclosed,
        "jobDescription": parsed.get("summary"),
        "jobEndDate": None,
        "jobId": pinpoint_id,
        "jobInitDate": now,
        "jobTitle": title_id,
        "jobTitleForFrontend": job.get("jobTitleForFrontend") or (raw_job.get("jobTitleForFrontend") if raw_job else None),
        "jobType": job.get("jobType", "pinpoint"),
        "location": location_geojson,
        "majorDisciplineIds": [],
        "maxSalary": max_salary,
        "minSalary": min_salary,
        "minorDisciplineIds": [],
        "qualifications": qualification_ids,
        "rate": rate,
        "skillRequired": skill_required,
        "updatedAt": now,
        "updatedBy": "system",
        "workArrangement": work_arrangement,
        "sourceKey": source_key,
        "validated": True,
        "synced": False,
    }

    jobs.update_one({"sourceKey": doc["sourceKey"]}, {"$set": doc}, upsert=True)

    queue.update_one(
        {"_id": job["_id"]},
        {"$set": {"mapped": True, "mappedAt": datetime.now(timezone.utc)}}
    )

    mapped += 1
    qual_count = len(qualification_ids)
    skill_count = len(skill_required) + len(good_to_have_skills)
    total_skills += skill_count
    total_qualifications += qual_count

    log_debug(f"MAPPED: {source_key} | title={job.get('jobTitleForFrontend', '')[:40]} | company={company_name[:20]} | skills={skill_count} | quals={qual_count}")
    log(f"   Inserted | skills={skill_count} | qualifications={qual_count}")

log("Flushing pending skill alias updates...")
alias_updates_count = flush_pending_alias_updates()
log(f"Updated {alias_updates_count} skills with new aliases")

end_time = datetime.now(timezone.utc)
duration = (end_time - start_time).total_seconds()

log("-" * 60)
log("SUMMARY")
log("-" * 60)
log(f"Total jobs found: {len(parsed_jobs)}")
log(f"Jobs mapped: {mapped}")
log(f"Jobs skipped: {len(parsed_jobs) - mapped}")
log(f"Total skills assigned: {total_skills}")
log(f"Total qualifications: {total_qualifications}")
log(f"Skill aliases updated: {alias_updates_count}")
log(f"Fuzzy matches cached: {len(skill_fuzzy_cache)}")
log(f"Duration: {duration:.2f}s")
log(f"Rate: {mapped/duration:.2f} jobs/sec" if duration > 0 else "Rate: N/A")
log("-" * 60)
log("Canonical mapping complete")
log(f"Log: {LOG_FILE}")

log_debug("=" * 60)
summary_json = {
    "run_date": TODAY_DATE,
    "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
    "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
    "duration_seconds": round(duration, 2),
    "total_jobs_found": len(parsed_jobs),
    "jobs_mapped": mapped,
    "jobs_skipped": len(parsed_jobs) - mapped,
    "total_skills_assigned": total_skills,
    "total_qualifications": total_qualifications,
    "skill_aliases_updated": alias_updates_count,
    "fuzzy_matches_cached": len(skill_fuzzy_cache),
    "companies_loaded": len(company_cache),
    "job_titles_loaded": len(job_title_ids),
    "skills_loaded": len(skill_ids),
    "rate_jobs_per_sec": round(mapped / duration, 2) if duration > 0 else 0,
    "log_file": LOG_FILE
}
log_debug(json.dumps(summary_json, indent=2))
log_debug("=" * 60)
