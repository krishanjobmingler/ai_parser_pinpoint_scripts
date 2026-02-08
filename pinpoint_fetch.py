#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
import aiohttp
from langdetect import detect, LangDetectException

US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware",
    "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
    "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming", "district of columbia",
    "puerto rico", "guam", "american samoa",
}

US_STATE_CODES = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky",
    "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd",
    "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
}

US_CITIES = {
    "new york city", "nyc", "los angeles", "chicago", "houston", "phoenix", "philadelphia",
    "san antonio", "san diego", "dallas", "san jose", "jacksonville", "fort worth",
    "san francisco", "indianapolis", "seattle", "denver", "boston", "austin",
    "nashville", "detroit", "las vegas", "memphis", "louisville", "baltimore",
    "milwaukee", "albuquerque", "tucson", "fresno", "sacramento", "kansas city", "atlanta",
    "miami", "raleigh", "omaha", "oakland", "minneapolis", "tulsa", "cleveland", "wichita",
    "tampa", "honolulu", "anaheim", "santa ana", "corpus christi", "portland",
    "st. louis", "pittsburgh", "anchorage", "stockton", "cincinnati",
    "saint paul", "greensboro", "plano", "brooklyn", "manhattan", "queens", "bronx",
    "staten island", "silicon valley", "charlotte", "orlando", "columbus",
    "arlington", "aurora", "henderson", "lincoln", "jersey city", "newark", "riverside",
}

CA_PROVINCES = {
    "alberta", "british columbia", "manitoba", "new brunswick", "newfoundland and labrador",
    "nova scotia", "ontario", "prince edward island", "quebec", "saskatchewan",
    "northwest territories", "nunavut", "yukon",
}

CA_PROVINCE_CODES = {"ab", "bc", "mb", "nb", "nl", "ns", "on", "pe", "qc", "sk", "nt", "nu", "yt"}

CA_CITIES = {
    "toronto", "montreal", "vancouver", "calgary", "edmonton", "ottawa", "winnipeg", "quebec city",
    "kitchener", "oshawa", "saskatoon", "regina", "st. john's", "barrie", "kelowna", "abbotsford",
    "greater toronto area", "gta", "hamilton", "halifax", "victoria", "windsor",
}

COUNTRY_KEYWORDS = {
    "united states", "u.s.", "u.s.a.", "usa", "united states of america",
    "canada", "north america", "(us)", "(usa)", "(canada)",
}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()


def _has_state_code(text: str, codes: set) -> bool:
    text_upper = text.upper().strip()
    for code in codes:
        cu = code.upper()
        if re.search(rf",\s*{cu}\b", text_upper): return True
        if re.search(rf"\({cu}\)", text_upper): return True
        if re.search(rf"-\s*{cu}\b", text_upper): return True
        if re.search(rf"\s{cu}$", text_upper): return True
        if re.search(rf"^{cu}$", text_upper): return True
        if re.search(rf"\s{cu}\s", text_upper): return True
    return False


def is_us_canada(raw_location: str) -> bool:
    if not raw_location:
        return False

    loc = _norm(raw_location)

    if any(kw in loc for kw in COUNTRY_KEYWORDS):
        return True

    if any(state in loc for state in US_STATES):
        return True

    if any(prov in loc for prov in CA_PROVINCES):
        return True

    if any(city in loc for city in US_CITIES):
        return True

    if any(city in loc for city in CA_CITIES):
        return True

    parts = re.split(r"[,;|/]", loc)
    for p in parts:
        p = p.strip()
        if _has_state_code(p, US_STATE_CODES):
            return True
        if _has_state_code(p, CA_PROVINCE_CODES):
            return True

    if loc in ("remote", "remote - us", "remote - usa", "remote - canada", "remote usa", "remote canada"):
        return True
    if "remote" in loc and any(kw in loc for kw in COUNTRY_KEYWORDS):
        return True

    return False


def is_english(text: str) -> bool:
    if not text or len(text.strip()) < 20:
        return True
    try:
        return detect(text[:500]) == "en"
    except LangDetectException:
        return True


PLACEHOLDER_PATTERNS = [
    r"area\s+(one|two|three|four|five|six|seven|eight|nine|ten|\d+)",
    r"responsibility\s+(one|two|three|four|five|six|seven|eight|nine|ten|\d+)",
    r"requirement\s+(one|two|three|four|five|six|seven|eight|nine|ten|\d+)",
    r"lorem\s+ipsum",
    r"\[insert\s+.+\]",
    r"\{insert\s+.+\}",
    r"tbd|to\s+be\s+determined|to\s+be\s+added",
    r"placeholder",
]
PLACEHOLDER_REGEX = re.compile("|".join(PLACEHOLDER_PATTERNS), re.IGNORECASE)


def is_placeholder_content(text: str) -> bool:
    if not text:
        return True
    text_clean = text.strip().lower()
    if len(text_clean) < 100:
        return True
    if len(PLACEHOLDER_REGEX.findall(text_clean)) >= 3:
        return True
    words = re.findall(r'\b[a-z]{3,}\b', text_clean)
    if len(words) > 10 and len(set(words)) / len(words) < 0.3:
        return True
    return False


ATLAS_URI = os.getenv("ATLAS_URI", "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority")
ATLAS_DB = os.getenv("ATLAS_DB", "jobminglr")
CUSTOMERS_COLLECTION = os.getenv("CUSTOMERS_COLLECTION", "pinpoint_customers")

LOCAL_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
LOCAL_DB = os.getenv("DB_NAME", "jobminglr_staging")

MAX_CONCURRENT = int(os.getenv("FETCH_MAX_CONCURRENT", "100"))
REQUEST_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_BASE_DIR = os.path.join(SCRIPT_DIR, "logs")
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOGS_BASE_DIR, TODAY_DATE)
LOG_FILE = os.path.join(LOG_DIR, "fetch.log")

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("pinpoint_fetch")
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


def clean_html(html):
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


async def load_companies(customers_col):
    cursor = customers_col.find({"status": "active"}, {"_id": 1, "company_name": 1, "board_token": 1})
    companies = []
    async for doc in cursor:
        company_id = doc.get("_id")
        company_name = doc.get("company_name", "").strip()
        board_token = doc.get("board_token", "").strip()
        if company_name and board_token:
            companies.append((company_name, board_token, company_id))
    return companies


async def fetch_jobs(session: aiohttp.ClientSession, token: str):
    url = f"https://{token}.pinpointhq.com/jobs.json"
    try:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.get(url, timeout=timeout) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if isinstance(data, dict):
                return data.get("data") or data.get("jobs") or []
            elif isinstance(data, list):
                return data
            return []
    except Exception as e:
        log(f"Error fetching {token}: {e}")
        return []


async def save_jobs(jobs_col, company, token, company_id, jobs_list):
    if not jobs_list:
        return 0, 0, 0, 0

    n = 0
    skipped_location = 0
    skipped_language = 0
    skipped_placeholder = 0

    for j in jobs_list:
        if not isinstance(j, dict):
            continue
        jid = j.get("id") or j.get("uuid") or j.get("slug")
        if not jid:
            continue

        location = j.get("location", {}).get("name") if isinstance(j.get("location"), dict) else j.get("location")
        title = j.get("title") or ""

        if not is_us_canada(location):
            skipped_location += 1
            log_debug(f"SKIP location: {title[:40]} | loc={location}")
            continue

        raw_sections = {
            "description": j.get("description"),
            "key_responsibilities": j.get("key_responsibilities"),
            "skills_knowledge_expertise": j.get("skills_knowledge_expertise"),
            "benefits": j.get("benefits"),
        }

        job_desc = clean_html(raw_sections["description"])
        responsibilities = clean_html(raw_sections["key_responsibilities"])
        expertise = clean_html(raw_sections["skills_knowledge_expertise"])
        benefits = clean_html(raw_sections["benefits"])

        combined_text = f"{title} {job_desc}"
        if not is_english(combined_text):
            skipped_language += 1
            log_debug(f"SKIP language: {title[:40]} | non-English")
            continue

        full_content = f"{job_desc} {responsibilities} {expertise}"
        if is_placeholder_content(full_content):
            skipped_placeholder += 1
            log_debug(f"SKIP placeholder: {title[:40]}")
            continue

        department = j.get("department", {}).get("name") if isinstance(j.get("department"), dict) else j.get("department")
        division = j.get("division", {}).get("name") if isinstance(j.get("division"), dict) else j.get("division")

        await jobs_col.update_one(
            {"sourceKey": f"{token}_{jid}"},
            {
                "$setOnInsert": {
                    "parsed": False,
                    "createdAt": datetime.now(timezone.utc),
                },
                "$set": {
                    "sourceKey": f"{token}_{jid}",
                    "companyId": company_id,
                    "companyName": company,
                    "boardToken": token,
                    "jobTitleForFrontend": j.get("title"),
                    "absoluteUrl": j.get("url"),
                    "employmentType": j.get("employment_type_text") or j.get("employment_type"),
                    "workplaceType": j.get("workplace_type_text") or j.get("workplace_type"),
                    "department": department,
                    "division": division,
                    "location": location,
                    "compensation": j.get("compensation"),
                    "compensationMinimum": j.get("compensation_minimum"),
                    "compensationMaximum": j.get("compensation_maximum"),
                    "compensationCurrency": j.get("compensation_currency"),
                    "compensationFrequency": j.get("compensation_frequency"),
                    "isSalaryDisclosed": bool(j.get("compensation_minimum") or j.get("compensation_maximum")),
                    "jobDescription": job_desc,
                    "keyResponsibilities": responsibilities,
                    "skillsKnowledgeExpertise": expertise,
                    "benefits": benefits,
                    "rawSections": raw_sections,
                    "fetchedAt": datetime.now(timezone.utc),
                    "jobType": "pinpoint",
                }
            },
            upsert=True
        )
        n += 1
        job_url = j.get('url') or j.get('absolute_url') or 'N/A'
        log_debug(f"SAVED: {title[:40]} | company={company[:20]} | loc={location[:30] if location else 'N/A'} | url={job_url}")

    return n, skipped_location, skipped_language, skipped_placeholder


async def process_company(session, jobs_col, company, token, company_id, semaphore, index, total):
    async with semaphore:
        jobs = await fetch_jobs(session, token)
        if not jobs:
            log(f"[{index}/{total}] No jobs for {company}")
            return {"company": company, "status": "no_jobs", "jobs_count": 0, "skipped_location": 0, "skipped_language": 0, "skipped_placeholder": 0}

        saved, skipped_loc, skipped_lang, skipped_placeholder = await save_jobs(jobs_col, company, token, company_id, jobs)

        skip_info = ""
        if skipped_loc > 0 or skipped_lang > 0 or skipped_placeholder > 0:
            skip_info = f" (skipped: loc={skipped_loc}, lang={skipped_lang}, placeholder={skipped_placeholder})"

        log(f"[{index}/{total}] {company} -- {saved} jobs saved{skip_info}")
        return {"company": company, "status": "success", "jobs_count": saved, "skipped_location": skipped_loc, "skipped_language": skipped_lang, "skipped_placeholder": skipped_placeholder}


async def main():
    start_time = datetime.now(timezone.utc)
    log("Starting Pinpoint job fetch (ASYNC)")
    log(f"Max concurrent: {MAX_CONCURRENT}")
    log("Filter: USA + Canada only | English only")

    atlas_client = AsyncIOMotorClient(ATLAS_URI)
    local_client = AsyncIOMotorClient(LOCAL_URI)

    customers_col = atlas_client[ATLAS_DB][CUSTOMERS_COLLECTION]
    jobs_col = local_client[LOCAL_DB]["pinpoint_jobs_raw"]

    companies = await load_companies(customers_col)
    total = len(companies)
    log(f"Loaded {total} active companies")

    if not companies:
        log("No active companies found")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=10)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            process_company(session, jobs_col, company, token, company_id, semaphore, i + 1, total)
            for i, (company, token, company_id) in enumerate(companies)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()

    success = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
    no_jobs = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "no_jobs")
    errors = sum(1 for r in results if isinstance(r, Exception))
    total_jobs = sum(r.get("jobs_count", 0) for r in results if isinstance(r, dict))
    total_skipped_location = sum(r.get("skipped_location", 0) for r in results if isinstance(r, dict))
    total_skipped_language = sum(r.get("skipped_language", 0) for r in results if isinstance(r, dict))
    total_skipped_placeholder = sum(r.get("skipped_placeholder", 0) for r in results if isinstance(r, dict))

    log("-" * 60)
    log("SUMMARY")
    log("-" * 60)
    log(f"Companies: {total} | Success: {success} | No jobs: {no_jobs} | Errors: {errors}")
    log(f"Jobs saved: {total_jobs}")
    log(f"Skipped: loc={total_skipped_location} | lang={total_skipped_language} | placeholder={total_skipped_placeholder}")
    log(f"Duration: {duration:.2f}s | Rate: {total/duration:.2f} companies/sec")
    log("-" * 60)
    log(f"Log: {LOG_FILE}")

    log_debug("=" * 60)
    summary_json = {
        "run_date": TODAY_DATE,
        "duration_seconds": round(duration, 2),
        "total_companies": total,
        "companies_success": success,
        "total_jobs_saved": total_jobs,
        "skipped_location": total_skipped_location,
        "skipped_language": total_skipped_language,
        "skipped_placeholder": total_skipped_placeholder,
        "rate_companies_per_sec": round(total / duration, 2) if duration > 0 else 0,
    }
    log_debug(json.dumps(summary_json, indent=2))
    log_debug("=" * 60)

    atlas_client.close()
    local_client.close()


if __name__ == "__main__":
    asyncio.run(main())
