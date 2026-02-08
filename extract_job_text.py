#!/usr/bin/env python3

from pymongo import MongoClient

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"

client = MongoClient(PROD_URI)
db = client[PROD_DB]
jobs_col = db["jobs"]

def extract_job_texts(limit=None):
    """Extract job title and description as combined text."""
    query = {"isDeleted": {"$ne": True}}
    projection = {"jobTitleForFrontend": 1, "jobDescription": 1}
    
    cursor = jobs_col.find(query, projection)
    if limit:
        cursor = cursor.limit(limit)
    
    texts = []
    for job in cursor:
        title = job.get("jobTitleForFrontend", "") or ""
        desc = job.get("jobDescription", "") or ""
        combined = f"{title}\n{desc}".strip()
        if combined:
            texts.append(combined)
    
    return texts

def get_single_text(limit=None):
    """Get all jobs as single concatenated text."""
    texts = extract_job_texts(limit)
    return "\n\n---\n\n".join(texts)

if __name__ == "__main__":
    print(f"📡 Connected to production: {PROD_DB}.jobs")
    
    total = jobs_col.count_documents({"isDeleted": {"$ne": True}})
    print(f"📋 Total jobs: {total}")
    
    texts = extract_job_texts(limit=5)
    print(f"\n🔍 Sample (first 5 jobs):\n")
    
    for i, text in enumerate(texts, 1):
        print(f"--- Job {i} ---")
        print(text[:500] + "..." if len(text) > 500 else text)
        print()

