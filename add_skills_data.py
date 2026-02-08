#!/usr/bin/env python3

import csv
import ast
from datetime import datetime, timezone
from pymongo import MongoClient

CSV_FILE = "resume_data.csv"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "jobminglr_staging"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
skills_col = db["skills"]

print(f"📂 Reading CSV: {CSV_FILE}")

all_skills = set()

with open(CSV_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        skills_str = row.get('skills') or row.get('Skills') or ''
        if not skills_str:
            continue
        
        try:
            skills_list = ast.literal_eval(skills_str)
            if isinstance(skills_list, list):
                for skill in skills_list:
                    skill = str(skill).strip()
                    if skill and len(skill) > 1:
                        all_skills.add(skill)
        except (ValueError, SyntaxError):
            skills_list = [s.strip() for s in skills_str.split(',')]
            for skill in skills_list:
                if skill and len(skill) > 1:
                    all_skills.add(skill)

print(f"📋 Found {len(all_skills)} unique skills in CSV")

existing_skills = set()
for s in skills_col.find({}, {"name": 1}):
    if s.get("name"):
        existing_skills.add(s["name"].lower())

print(f"📚 Found {len(existing_skills)} existing skills in DB")

inserted = 0
skipped = 0

for skill_name in sorted(all_skills):
    if skill_name.lower() in existing_skills:
        skipped += 1
        continue
    
    now = datetime.now(timezone.utc)
    doc = {
        "name": skill_name,
        "isDeleted": False,
        "createdBy": "system",
        "updatedBy": "system",
        "createdAt": now,
        "updatedAt": now,
        "aliases": [skill_name.lower()]
    }
    
    skills_col.insert_one(doc)
    existing_skills.add(skill_name.lower())
    inserted += 1
    print(f"   ✅ Added: {skill_name}")

print(f"\n🏁 Done — {inserted} inserted, {skipped} already existed")

