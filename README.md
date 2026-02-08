# AI Parser - Pinpoint Job Pipeline Scripts

Automated pipeline for fetching, parsing, mapping, and syncing job postings from **Pinpoint HQ** into the **JobMinglr** production database.

---

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Architecture Flow](#architecture-flow)
- [Setup & Installation](#setup--installation)
- [Environment Variables](#environment-variables)
- [Running the Pipeline](#running-the-pipeline)
- [Script Reference](#script-reference)
  - [Core Pipeline Scripts](#core-pipeline-scripts)
  - [Utility Scripts](#utility-scripts)
- [Database Details](#database-details)
  - [Staging Database](#staging-database-jobminglr_staging)
  - [Production Database](#production-database-jobminglr)
- [Logs](#logs)
- [Reset & Maintenance](#reset--maintenance)

---

## Pipeline Overview

The pipeline processes Pinpoint ATS (Applicant Tracking System) job postings through 5 stages:

```
Fetch --> Parse --> Canonical Map --> Validate --> Sync to Production
```

| Stage | Script | Description |
|-------|--------|-------------|
| 1. Fetch | `pinpoint_fetch.py` | Async fetch jobs from Pinpoint API (USA/Canada + English only) |
| 2. Parse | `pinpoint_parse_ai.py` | AI-powered skill extraction + qualification parsing |
| 3. Map | `canonical_mapper.py` | Fuzzy match to production canonical data (companies, job titles, skills) |
| 4. Validate | `run_pipeline.sh` (mongosh) | Auto-validate all mapped jobs |
| 5. Sync | `production_sync_batch.py` | Bulk upsert validated jobs to production MongoDB Atlas |

---

## Architecture Flow

```
                        +-----------------------+
                        |   Pinpoint HQ API     |
                        |  (jobs.json per board) |
                        +-----------+-----------+
                                    |
                          Stage 1: FETCH
                      (pinpoint_fetch.py - async)
                                    |
                                    v
                    +-------------------------------+
                    |   Staging: pinpoint_jobs_raw   |
                    |   (local MongoDB)              |
                    +---------------+---------------+
                                    |
                          Stage 2: PARSE
                   (pinpoint_parse_ai.py + new_extraction.py)
                     - Enrich API (qualifications)
                     - FlashText skill matching
                                    |
                                    v
                    +-------------------------------+
                    |  Staging: pinpoint_jobs_queue  |
                    |  (parsed + skills + quals)     |
                    +---------------+---------------+
                                    |
                          Stage 3: MAP
                     (canonical_mapper.py)
                     - Match companies (create if new)
                     - Fuzzy match job titles
                     - Fuzzy match skills (RapidFuzz)
                     - Auto-add skill aliases
                                    |
                                    v
                    +-------------------------------+
                    |     Staging: jobs              |
                    |  (production-ready schema)     |
                    +---------------+---------------+
                                    |
                       Stage 4: VALIDATE
                    (mongosh auto-validate)
                                    |
                                    v
                          Stage 5: SYNC
                   (production_sync_batch.py)
                     - Bulk upsert to Atlas
                     - Multi-threaded (4 workers)
                                    |
                                    v
                    +-------------------------------+
                    |   Production: jobs             |
                    |   (MongoDB Atlas - jobminglr)  |
                    +-------------------------------+
```

---

## Setup & Installation

### Prerequisites

- **Python** 3.10+
- **MongoDB** (local instance on `localhost:27017`)
- **MongoDB Atlas** account (production database)
- **mongosh** (MongoDB Shell) for validation step
- **pip** / **venv** for Python dependencies

### 1. Clone the Repository

```bash
git clone https://github.com/krishanjobmingler/ai_parser_pinpoint_scripts.git
cd ai_parser_pinpoint_scripts
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install motor aiohttp langdetect beautifulsoup4 pymongo rapidfuzz flashtext psutil bson python-dotenv requests tqdm boto3 pydantic deep-translator
```

### 4. Start Local MongoDB

```bash
# If using systemd
sudo systemctl start mongod

# Or with Docker
docker run -d -p 27017:27017 --name mongodb mongo:7
```

### 5. Verify Connectivity

```bash
# Test local MongoDB
mongosh mongodb://localhost:27017 --eval "db.adminCommand('ping')"

# Test production Atlas (requires network access)
mongosh "mongodb+srv://..." --eval "db.adminCommand('ping')"
```

---

## Environment Variables

All scripts use sensible defaults but can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | Local staging MongoDB URI |
| `DB_NAME` | `jobminglr_staging` | Local staging database name |
| `ATLAS_URI` | *(hardcoded)* | Production MongoDB Atlas URI |
| `ATLAS_DB` | `jobminglr` | Production database name |
| `FETCH_MAX_CONCURRENT` | `100` | Max concurrent HTTP requests during fetch |
| `FETCH_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `ENRICH_BATCH_URL` | `http://localhost:8001/enrich/batch` | Enrich API endpoint for qualifications |
| `ENRICH_BATCH_SIZE` | `500` | Jobs per AI parsing batch |
| `ENABLE_VECTOR_SEARCH` | `false` | Enable vector similarity search |
| `ENRICH_VECTOR_THRESHOLD` | `0.70` | Vector search similarity threshold |
| `USE_LOCAL_SKILL_MATCHING` | `true` | Use FlashText local skill matching |
| `ENRICH_TIMEOUT` | `0` (unlimited) | Enrich API timeout (seconds) |
| `ENRICH_RETRIES` | `3` | API retry count on failure |

---

## Running the Pipeline

### Full Pipeline (Recommended)

```bash
bash run_pipeline.sh
```

This runs all 5 stages in order:
1. `pinpoint_fetch.py` - Fetch new jobs
2. `pinpoint_parse_ai.py` - Parse & extract skills
3. `canonical_mapper.py` - Map to canonical data
4. Auto-validate (mongosh)
5. `production_sync_batch.py` - Sync to production

### Individual Stages

```bash
# Stage 1: Fetch only
python3 pinpoint_fetch.py

# Stage 2: Parse only
python3 pinpoint_parse_ai.py

# Stage 3: Canonical mapping only
python3 canonical_mapper.py

# Stage 4: Validate only
mongosh "mongodb://localhost:27017/jobminglr_staging" --eval '
  db.jobs.updateMany(
    { validated: { $ne: true } },
    { $set: { validated: true }, $unset: { synced: "" } }
  )
'

# Stage 5: Sync to production only
python3 production_sync_batch.py
```

### Pipeline Orchestrator (Advanced)

```bash
python3 pinpoint_pipeline.py
```

This provides:
- Lock file to prevent overlapping runs
- CPU/RAM resource monitoring per stage
- Detailed stage-by-stage timing breakdown
- DB document count tracking

---

## Script Reference

### Core Pipeline Scripts

| Script | Language | Description |
|--------|----------|-------------|
| `run_pipeline.sh` | Bash | Simple pipeline orchestrator - runs all stages sequentially |
| `pinpoint_pipeline.py` | Python | Advanced orchestrator with resource monitoring and lock file |
| `pinpoint_fetch.py` | Python (async) | Fetches jobs from Pinpoint HQ API for all active companies. Filters: USA/Canada locations, English language, non-placeholder content |
| `pinpoint_parse_ai.py` | Python (async) | Parses job descriptions using Enrich API (qualifications) + FlashText local skill matching. Processes in configurable batch sizes |
| `new_extraction.py` | Python | FlashText-based skill extractor with intelligent weighting (position, frequency, required/preferred context). Loaded from MongoDB `skills` collection |
| `canonical_mapper.py` | Python | Maps parsed jobs to production canonical data using RapidFuzz fuzzy matching. Creates missing companies, matches job titles and skills with aliases |
| `production_sync_batch.py` | Python | Multi-threaded bulk sync of validated jobs to production MongoDB Atlas. 4 workers, 1000 jobs per batch |

### Utility Scripts

| Script | Language | Description |
|--------|----------|-------------|
| `pinpoint_inactive_sync.py` | Python | Deactivates production jobs that no longer exist in staging |
| `production_sync_test.py` | Python | Interactive single-job sync for testing (prompts for job `_id`) |
| `pinpoint_reset_pipeline.sh` | Bash | Drops all local staging collections (full reset). Does NOT affect production |
| `pinpoint_sync_taxonomy.sh` | Bash | **DEPRECATED** - Taxonomy sync is now handled by `canonical_mapper.py` |
| `add_skills_data.py` | Python | Imports skills from a CSV file (`resume_data.csv`) into staging |
| `sync_job_titles_to_prod.py` | Python | Syncs auto-created job titles (SOC code `99-*`) from staging to production |
| `embed_skills_bedrock.py` | Python | Generates skill embeddings using AWS Bedrock (Titan Embed v1) and stores in MongoDB |
| `embedding.py` | Python | Triggers local embedding/indexing service via HTTP POST |
| `embedding_local_model.py` | Python | Same as `embedding.py` (local embedding service trigger) |
| `extract_skills_llm.py` | Python | LLM-based skill extraction from production jobs using Gemini. Stores results in `New_skills` collection |
| `extract_job_text.py` | Python | Extracts job title + description text from production for analysis |
| `cleanup_noise_skills.py` | Python | LLM-powered skill classifier that removes noise/garbage entries from `New_skills` collection |
| `translate_skills.py` | Python | Translates non-English skill names to English using Google Translator |

---

## Database Details

### Staging Database (`jobminglr_staging`)

Local MongoDB instance at `mongodb://localhost:27017`

#### `pinpoint_jobs_raw`

Raw jobs fetched from Pinpoint API. One document per job posting.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Auto-generated |
| `sourceKey` | String | Unique key: `{board_token}_{job_id}` |
| `companyId` | ObjectId | Reference to Atlas `pinpoint_customers._id` |
| `companyName` | String | Company display name |
| `boardToken` | String | Pinpoint board token |
| `jobTitleForFrontend` | String | Job title as shown on Pinpoint |
| `absoluteUrl` | String | Job posting URL |
| `employmentType` | String | Full-time, part-time, contract, etc. |
| `workplaceType` | String | Remote, hybrid, on-site |
| `department` | String | Department name |
| `division` | String | Division name |
| `location` | String | Job location (city, state) |
| `compensation` | String | Compensation text |
| `compensationMinimum` | Number | Min salary |
| `compensationMaximum` | Number | Max salary |
| `compensationCurrency` | String | Currency code (USD, CAD) |
| `compensationFrequency` | String | year, month, hour |
| `isSalaryDisclosed` | Boolean | Whether salary is visible |
| `jobDescription` | String | Cleaned description text |
| `keyResponsibilities` | String | Cleaned responsibilities text |
| `skillsKnowledgeExpertise` | String | Cleaned skills/requirements text |
| `benefits` | String | Cleaned benefits text |
| `rawSections` | Object | Original HTML sections from API |
| `parsed` | Boolean | Whether job has been AI-parsed |
| `fetchedAt` | Date | Timestamp of fetch |
| `createdAt` | Date | First insertion timestamp |

#### `pinpoint_jobs_queue`

Parsed jobs with extracted skills and qualifications, awaiting canonical mapping.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Auto-generated |
| `sourceKey` | String | Same as `pinpoint_jobs_raw.sourceKey` |
| `companyId` | ObjectId | Company reference |
| `jobTitleForFrontend` | String | Job title |
| `employmentType` | String | Normalized employment type |
| `workplaceType` | String | Normalized workplace type |
| `location` | String | Job location |
| `jobType` | String | Always `"pinpoint"` |
| `parsed` | Boolean | Always `true` |
| `ai_parsed` | Object | AI parsing results (see below) |
| `ai_parsed.skills` | Array | Extracted skills with weights |
| `ai_parsed.skills[].skill_id` | String | MongoDB skill `_id` |
| `ai_parsed.skills[].name` | String | Skill display name |
| `ai_parsed.skills[].weight` | Number | Importance weight (1-5) |
| `ai_parsed.skills[].required` | Boolean | Required vs nice-to-have |
| `ai_parsed.qualificationIds` | Array | Matched qualification ObjectIds |
| `ai_parsed.summary` | String | Job text summary (max 4000 chars) |
| `ai_parsed.parsedAt` | Date | Parse timestamp |
| `qualificationIds` | Array | Top-level qualification IDs |
| `validated` | Boolean | Auto-set to `true` |
| `mapped` | Boolean | Whether canonical mapping is done |
| `mappedAt` | Date | Mapping timestamp |

#### `jobs`

Production-ready jobs with canonical references. Final staging collection before sync.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Auto-generated |
| `sourceKey` | String | Unique source identifier |
| `absoluteUrl` | String | Job posting URL |
| `address` | String | Job location address |
| `benefits` | Array | List of benefit descriptions |
| `company` | ObjectId | Production `companies._id` |
| `companyCulture` | Array | Culture tags (empty by default) |
| `employmentType` | String | `full-time`, `part-time`, `contract`, `temporary`, `internship` |
| `experienceLevel` | String | `intermediate` (default) |
| `skillRequired` | Array | Required skills (see below) |
| `skillRequired[].skillId` | ObjectId | Production `skills._id` |
| `skillRequired[].weight` | Number | Importance (1-5) |
| `goodToHaveSkill` | Array | Nice-to-have skills (same schema as `skillRequired`) |
| `qualifications` | Array | Array of qualification ObjectIds |
| `jobTitle` | ObjectId | Production `job-titles._id` |
| `jobTitleForFrontend` | String | Display title |
| `jobDescription` | String | Cleaned description text |
| `jobId` | String | Pinpoint job ID (from URL) |
| `jobType` | String | Always `"pinpoint"` |
| `jobInitDate` | Date | Job creation date |
| `jobEndDate` | Date | `null` (no expiry) |
| `minSalary` | Number | Minimum salary |
| `maxSalary` | Number | Maximum salary |
| `rate` | String | `year`, `month`, `week`, `hour`, `day` |
| `isSalaryDisclosed` | Boolean | Salary visibility |
| `workArrangement` | String | `Remote`, `Hybrid`, `In-Office` |
| `location` | Object | GeoJSON `{ type: "Point", coordinates: [lng, lat] }` |
| `isActive` | Boolean | Always `true` |
| `isDeleted` | Boolean | Always `false` |
| `isAddedFromGreenhouse` | Boolean | Always `false` |
| `validated` | Boolean | `true` when ready for sync |
| `synced` | Boolean | `true` after production sync |
| `syncedAt` | Date | Sync timestamp |
| `createdBy` | ObjectId | System user ID |
| `updatedBy` | String | `"system"` |
| `createdAt` | Date | Creation timestamp |
| `updatedAt` | Date | Last update timestamp |

#### `skills`

Local copy of production skills used by FlashText skill extractor.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Skill ID |
| `name` | String | Canonical skill name |
| `aliases` | Array | Alternative names/spellings |
| `isDeleted` | Boolean | Soft delete flag |
| `embedding` | Array | Vector embedding (if generated) |

#### `ai_parsing_debug`

Debug data for AI parsing batches. Used for troubleshooting.

| Field | Type | Description |
|-------|------|-------------|
| `batch_index` | Number | Batch number |
| `timestamp` | Date | Processing timestamp |
| `date` | String | Date string (YYYY-MM-DD) |
| `jobs_count` | Number | Jobs in batch |
| `api_input` | Object | Truncated API request data |
| `api_output` | Object | API response summary |

---

### Production Database (`jobminglr`)

MongoDB Atlas cluster.

#### `pinpoint_customers`

Active Pinpoint customer companies (read by fetch stage).

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Customer ID |
| `company_name` | String | Company display name |
| `board_token` | String | Pinpoint board identifier |
| `status` | String | `"active"` for active companies |

#### `companies`

Production companies collection. Canonical mapper creates entries if not found.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Company ID |
| `name` | String | Company name |
| `logo` | String | Logo URL (default: `/splash.png`) |
| `size` | Number | Company size |
| `address` | String | Company address |
| `location` | Object | GeoJSON point |
| `category` | String | Company category |
| `websiteUrl` | String | Company website |
| `createdBy` | String | `"system"` |
| `createdAt` | Date | Creation timestamp |
| `updatedAt` | Date | Update timestamp |

#### `job-titles`

Canonical job titles with aliases. Used for fuzzy matching.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Job title ID |
| `title` | String | Canonical title |
| `aliases` | Array | Alternative title strings |
| `isDeleted` | Boolean | Soft delete flag |

#### `skills`

Production skills with aliases. Used for fuzzy matching and skill extraction.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Skill ID |
| `name` | String | Canonical skill name |
| `aliases` | Array | Alternative names (auto-updated by canonical mapper) |
| `isDeleted` | Boolean | Soft delete flag |
| `embedding` | Array | Vector embedding (optional) |

#### `jobs`

Production jobs collection. Final destination for pipeline output.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Job ID |
| `absoluteUrl` | String | Original job posting URL |
| `address` | String | Job location |
| `benefits` | Array | Benefits list |
| `company` | ObjectId | Reference to `companies._id` |
| `employmentType` | String | Employment type |
| `experienceLevel` | String | Experience level |
| `skillRequired` | Array | Required skills with weights |
| `goodToHaveSkill` | Array | Nice-to-have skills |
| `qualifications` | Array | Qualification ObjectIds |
| `jobTitle` | ObjectId | Reference to `job-titles._id` |
| `jobTitleForFrontend` | String | Display title |
| `jobDescription` | String | Job description text |
| `jobType` | String | `"pinpoint"` |
| `minSalary` | Number | Min salary |
| `maxSalary` | Number | Max salary |
| `rate` | String | Salary frequency |
| `isSalaryDisclosed` | Boolean | Salary visibility |
| `workArrangement` | String | Work arrangement |
| `location` | Object | GeoJSON point |
| `isActive` | Boolean | Active status |
| `isDeleted` | Boolean | Soft delete |
| `createdAt` | Date | Creation timestamp |
| `updatedAt` | Date | Update timestamp |

---

## Logs

All pipeline scripts write logs to:

```
scripts/logs/{YYYY-MM-DD}/
```

| Log File | Script |
|----------|--------|
| `fetch.log` | `pinpoint_fetch.py` |
| `ai_parsing.log` | `pinpoint_parse_ai.py` |
| `skill_extraction.log` | `new_extraction.py` |
| `canonical_mapper.log` | `canonical_mapper.py` |
| `production_sync.log` | `production_sync_batch.py` |
| `pipeline.log` | `pinpoint_pipeline.py` |

Each log includes:
- **Console**: INFO level (summary)
- **File**: DEBUG level (per-job details with function name and line number)

---

## Reset & Maintenance

### Full Pipeline Reset (Local Only)

```bash
bash pinpoint_reset_pipeline.sh
```

This drops **all** local staging collections. Production data is NOT affected.

### Deactivate Stale Production Jobs

```bash
python3 pinpoint_inactive_sync.py
```

Marks production jobs as `isActive: false` if they no longer exist in staging.

### Test Single Job Sync

```bash
python3 production_sync_test.py
```

Interactive script that prompts for a job `_id` and syncs it to production.

---

## Skill Extraction Details

The `new_extraction.py` module uses **FlashText** for high-performance skill matching with intelligent weighting:

| Factor | Weight Change | Description |
|--------|--------------|-------------|
| Base | 3 | Default weight for all matches |
| Frequency | +1 (max +1) | Bonus for multiple mentions |
| Position | +1 | Bonus if found in first 30% of text |
| Required context | +1 | Near keywords: "required", "must have", "essential" |
| Preferred context | -1 | Near keywords: "nice to have", "preferred", "optional" |
| **Range** | **1-5** | Final weight clamped to this range |

A **noisy canonical** blocklist filters out ~600+ common English words that could false-match skill names (e.g., "go", "make", "lead", "swift").

---

## License

Internal use only - JobMinglr.
