#!/usr/bin/env python3

import sys
import json
import asyncio
import os
from datetime import datetime, timezone
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from langchain.agents import create_agent

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"
TEST_DB = "test"

LOG_FILE = "/home/krishan/Fee_projects/Job_mingler/code/scripts/extract_skills_log.txt"

BATCH_SIZE = 100
LLM_CONCURRENCY = 20

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

class Skills(BaseModel):
    skills: Optional[List[str]] = Field(
        default=None,
        description="List of extracted skills in Title Case format",
        json_schema_extra={"example": ["Python", "SQL", "Machine Learning"]}
    )

if "GOOGLE_API_KEY" not in os.environ:
    os.environ["GOOGLE_API_KEY"] = ""

from langchain_google_genai import ChatGoogleGenerativeAI

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite",
    temperature=0.0,
    max_retries=3,
    api_key=os.environ["GOOGLE_API_KEY"],
)
skills_agent = create_agent(model=llm, response_format=Skills)

SYSTEM_PROMPT = """Extract skills from job text. Return JSON only.
Format: {"skills": ["Skill1", "Skill2"]}
Include: tools, software, languages, frameworks, methodologies, soft skills, certifications.
Exclude: job titles, company names, locations.
Use Title Case. No duplicates. If none: {"skills": null}"""

async def extract_skills(text: str) -> List[str]:
    """Extract skills from text using LLM with timeout."""
    try:
        result = await asyncio.wait_for(
            skills_agent.ainvoke({
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text[:6000]}
                ]
            }),
            timeout=30.0
        )
        skills_data = result["structured_response"].model_dump_json()
        skills = json.loads(skills_data)
        return skills.get("skills", []) or []
    except asyncio.TimeoutError:
        return []
    except Exception as e:
        return []

async def process_jobs(limit: int = 1000000, skip: int = 0):
    start_time = datetime.now()
    
    log("=" * 60)
    log(f"🚀 STARTED - Extract Skills LLM (Optimized)")
    log(f"⏱️  Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"📋 Limit: {limit} | Skip: {skip} | Concurrency: {LLM_CONCURRENCY}")
    
    client = AsyncIOMotorClient(PROD_URI, maxPoolSize=100)
    db = client[PROD_DB]
    test_db = client[TEST_DB]
    jobs_col = db["jobs"]
    new_skills_col = db["New_skills"]
    extraction_col = test_db["job_skill_extractions"]
    
    # Load existing skills into set
    existing_skills = set()
    async for s in new_skills_col.find({}, {"name": 1}):
        if s.get("name"):
            existing_skills.add(s["name"].lower())
    log(f"📚 Existing skills: {len(existing_skills)}")
    
    # Pre-fetch all jobs into memory
    log("📥 Loading jobs...")
    jobs_data = []
    cursor = jobs_col.find(
        {"isDeleted": {"$ne": True}},
        {"_id": 1, "jobTitleForFrontend": 1, "jobDescription": 1}
    ).skip(skip).limit(limit)
    
    async for job in cursor:
        job_id = str(job.get("_id", ""))
        title = job.get("jobTitleForFrontend", "") or ""
        desc = job.get("jobDescription", "") or ""
        text = f"{title}\n{desc}".strip()
        if text and len(text) >= 50:
            jobs_data.append((job_id, title, desc, text))
    
    total_jobs = len(jobs_data)
    log(f"📊 Jobs to process: {total_jobs}")
    
    all_extracted_skills = set()
    pending_skills = set()
    total_inserted = 0
    processed = skip
    
    # Process in batches
    for batch_start in range(0, total_jobs, LLM_CONCURRENCY):
        batch_end = min(batch_start + LLM_CONCURRENCY, total_jobs)
        batch = jobs_data[batch_start:batch_end]
        
        # Parallel LLM extraction with error handling
        tasks = [extract_skills(text) for _, _, _, text in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to empty lists
        results = [r if isinstance(r, list) else [] for r in results]
        
        # Prepare bulk insert docs
        now = datetime.now(timezone.utc)
        extraction_docs = []
        batch_skills_count = 0
        
        for (job_id, title, desc, _), skills in zip(batch, results):
            processed += 1
            batch_skills_count += len(skills)
            
            extraction_docs.append({
                "jobId": job_id,
                "jobTitle": title,
                "jobDescription": desc,
                "extractedSkills": skills,
                "createdAt": now,
            })
            
            for skill in skills:
                skill_lower = skill.lower()
                if skill_lower not in existing_skills and skill_lower not in pending_skills:
                    pending_skills.add(skill_lower)
                    all_extracted_skills.add(skill)
        
        # Bulk insert extraction records
        if extraction_docs:
            await extraction_col.insert_many(extraction_docs, ordered=False)
        
        log(f"[{batch_start + skip + 1}-{processed}] ✅ {len(batch)} jobs | {batch_skills_count} skills")
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.5)
        
        # Save new skills in batches
        if len(pending_skills) >= BATCH_SIZE:
            docs = [{
                "name": s,
                "isDeleted": False,
                "aliases": [s],
                "createdBy": "system",
                "updatedBy": "system",
                "createdAt": now,
                "updatedAt": now,
            } for s in pending_skills if s not in existing_skills]
            
            if docs:
                await new_skills_col.insert_many(docs, ordered=False)
                total_inserted += len(docs)
                existing_skills.update(pending_skills)
                log(f"   💾 Inserted {len(docs)} new skills")
            pending_skills.clear()
    
    # Save remaining skills
    if pending_skills:
        now = datetime.now(timezone.utc)
        docs = [{
            "name": s,
            "isDeleted": False,
            "aliases": [s],
            "createdBy": "system",
            "updatedBy": "system",
            "createdAt": now,
            "updatedAt": now,
        } for s in pending_skills if s not in existing_skills]
        
        if docs:
            await new_skills_col.insert_many(docs, ordered=False)
            total_inserted += len(docs)
            log(f"   💾 Inserted {len(docs)} final skills")
    
    end_time = datetime.now()
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    hours, remainder = divmod(int(total_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    log("=" * 60)
    log("📊 SUMMARY")
    log("=" * 60)
    log(f"📋 Jobs processed: {processed - skip}")
    log(f"🎯 Unique skills: {len(all_extracted_skills)}")
    log(f"💾 New skills inserted: {total_inserted}")
    log(f"⏱️  Duration: {hours}h {minutes}m {seconds}s")
    if total_seconds > 0:
        log(f"⚡ Rate: {(processed - skip) / total_seconds:.2f} jobs/sec")
    log("=" * 60)
    
    client.close()

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1000000
    skip = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    asyncio.run(process_jobs(limit=limit, skip=skip))
