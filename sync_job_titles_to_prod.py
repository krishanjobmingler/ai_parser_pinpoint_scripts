#!/usr/bin/env python3

from datetime import datetime, timezone
from pymongo import MongoClient

LOCAL_URI = "mongodb://localhost:27017"
LOCAL_DB = "jobminglr_staging"

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"

client_local = MongoClient(LOCAL_URI)
client_prod = MongoClient(PROD_URI)

local_titles = client_local[LOCAL_DB]["job-titles"]
prod_titles = client_prod[PROD_DB]["job-titles"]

print("🔄 Syncing job titles from local to production...")

new_titles = list(local_titles.find({"socCode": {"$regex": "^99-"}}))
print(f"📋 Found {len(new_titles)} auto-created job titles in local")

synced = 0
skipped = 0

for title in new_titles:
    title_name = title.get("title")
    
    existing = prod_titles.find_one(
        {"title": {"$regex": f"^{title_name}$", "$options": "i"}},
        {"_id": 1}
    )
    
    if existing:
        skipped += 1
        continue
    
    title_doc = title.copy()
    title_doc.pop("_id", None)
    
    prod_titles.insert_one(title_doc)
    synced += 1
    print(f"   ✅ Synced: {title_name}")

print(f"\n🏁 Done — {synced} synced, {skipped} already existed")

