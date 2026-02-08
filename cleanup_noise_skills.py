#!/usr/bin/env python3

import sys
import json
import asyncio
from datetime import datetime
from typing import List
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from langchain.agents import create_agent

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"
BATCH_SIZE = 500
LLM_CONCURRENCY = 500
LOG_FILE = "/home/krishan/Fee_projects/Job_mingler/code/scripts/cleanup_skills_log.txt"

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

class IsSkill(BaseModel):
    is_skill: bool = Field(..., description="True ONLY for specific, learnable skills in English (1-3 words). False for tasks, responsibilities, non-English, or generic terms.")

from langchain_nvidia_ai_endpoints import ChatNVIDIA

llm = ChatNVIDIA(base_url="http://icms-chat.runai-proj-haryana-kaushal-rozgar-nigam-ltd.inferencing.shakticloud.ai/v1", model="openai/gpt-oss-20b", temperature=0.0)
agent = create_agent(model=llm, response_format=IsSkill)

SYSTEM_PROMPT = """You are a skill classifier. Return: {"is_skill": true} or {"is_skill": false}

VALID SKILLS (return true):
- Technical: Python, JavaScript, SQL, AWS, Docker, Machine Learning, Data Analysis
- Tools/Software: Excel, Salesforce, SAP, Jira, Figma, Power BI, Tableau
- Business: Contract Negotiation, Due Diligence, Cost Analysis, Performance Analysis
- Finance: Financial Analysis, Budgeting, Forecasting, Risk Management, Accounting
- Management: Project Management, Team Leadership, Strategic Planning, Decision Making
- Legal: Contract Review, Compliance, Legal Research, Regulatory Knowledge
- Operations: Quality Control, Production Control, Supply Chain, Logistics
- HR: Recruitment, Training, Employee Relations, Performance Management
- Sales: Business Development, Client Relations, Sales Strategy, CRM
- Soft Skills: Communication, Leadership, Problem Solving, Critical Thinking
- Design: UX Design, UI Design, Graphic Design, CAD
- Engineering: Technical Inspection, Quality Assurance, Process Improvement
- Any professional ability that can be learned or developed

INVALID - NOT SKILLS (return false):
- Non-English text: Portuguese, Spanish, French phrases
- Random/hipster words: tofu, tacos, hoodie, vape, swag, meh, ugh, bespoke
- Locations: Portland, Brooklyn, USA, UK, EU, city names
- Very short (1-2 chars): pt, uk, us, eu
- Placeholder text: skill one, skill two, skill three
- Generic single words: new, one, level, next, best, other
- Person names or company names (unless software tools)

RULES:
1. Be INCLUSIVE - if it sounds like a professional skill, return true
2. Business/professional terms ARE valid skills
3. Analysis, Management, Control terms ARE valid skills
4. Only reject clear garbage (random words, non-English, locations)
5. When in doubt, return TRUE"""

async def check_skill(skill_name: str) -> bool:
    try:
        result = await asyncio.wait_for(
            agent.ainvoke({
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": skill_name}
                ]
            }),
            timeout=30.0
        )
        data = json.loads(result["structured_response"].model_dump_json())
        return data.get("is_skill", False)
    except Exception as e:
        log(f"   Error checking '{skill_name}': {e}")
        return True

async def process_batch(skills_batch: List[dict]) -> List[dict]:
    tasks = [check_skill(s["name"]) for s in skills_batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    to_delete = []
    for skill, is_valid in zip(skills_batch, results):
        if isinstance(is_valid, Exception):
            is_valid = True

        if not is_valid:
            to_delete.append(skill)
            log(f"   NOISE: '{skill['name']}'")
        else:
            log(f"   VALID: '{skill['name']}'")

    return to_delete

async def cleanup_skills(limit: int = 1000, dry_run: bool = True):
    start_time = datetime.now()

    log("=" * 60)
    log(f"CLEANUP NOISE SKILLS {'(DRY RUN)' if dry_run else '(LIVE DELETE)'}")
    log(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Limit: {limit} | Batch: {BATCH_SIZE}")
    log("=" * 60)

    client = AsyncIOMotorClient(PROD_URI)
    db = client[PROD_DB]
    skills_col = db["New_skills"]

    total_in_db = await skills_col.count_documents({})
    log(f"Total skills in DB: {total_in_db}")

    cursor = skills_col.find({}, {"_id": 1, "name": 1}).sort("createdAt", -1)
    if limit > 0:
        cursor = cursor.limit(limit)

    skills_list = await cursor.to_list(length=limit if limit > 0 else None)
    total_skills = len(skills_list)
    log(f"Skills to check: {total_skills}")

    total_deleted = 0
    processed = 0

    for batch_start in range(0, total_skills, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_skills)
        batch = skills_list[batch_start:batch_end]

        progress = (batch_end / total_skills) * 100
        log(f"\n[{batch_start + 1}-{batch_end}/{total_skills}] ({progress:.1f}%) Processing batch...")

        to_delete = await process_batch(batch)

        if to_delete and not dry_run:
            ids_to_delete = [s["_id"] for s in to_delete]
            result = await skills_col.delete_many({"_id": {"$in": ids_to_delete}})
            total_deleted += result.deleted_count
            log(f"   Deleted {result.deleted_count} skills")
        elif to_delete:
            total_deleted += len(to_delete)
            log(f"   Would delete {len(to_delete)} skills (dry run)")

        processed += len(batch)
        await asyncio.sleep(0.5)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"Skills checked: {processed}")
    log(f"Skills {'would be ' if dry_run else ''}deleted: {total_deleted}")
    log(f"Duration: {duration:.2f}s")
    log("=" * 60)

    if dry_run:
        log("This was a DRY RUN. Run with --delete to actually remove skills.")

    client.close()

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    dry_run = "--delete" not in sys.argv and "--apply" not in sys.argv

    print(f"Cleanup Noise Skills")
    print(f"   Limit: {limit if limit > 0 else 'ALL'}")
    print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE DELETE'}")
    print(f"   Batch: {BATCH_SIZE} | Concurrency: {LLM_CONCURRENCY}")
    print()

    asyncio.run(cleanup_skills(limit=limit, dry_run=dry_run))
