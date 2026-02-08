#!/usr/bin/env python3
"""
Deactivate Pinpoint jobs in production that no longer exist in staging.
"""

import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/pinpoint/.env"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
LOCAL_DB = os.getenv("MONGO_DB", "jobminglr_staging")
PROD_URI = os.getenv("PROD_MONGO_URI", "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority")
PROD_DB = "jobminglr-db"

client_local = MongoClient(MONGO_URI)
db_local = client_local[LOCAL_DB]
client_prod = MongoClient(PROD_URI)
db_prod = client_prod[PROD_DB]

jobs_ready = db_local["pinpoint_jobs_ready"]
jobs_prod = db_prod["jobs"]

print("🧹 Checking for inactive Pinpoint jobs...")

# 1️⃣ Get all sourceKeys in staging (latest active)
active_keys = set(j["sourceKey"] for j in jobs_ready.find({}, {"sourceKey": 1}))

# 2️⃣ Find currently active pinpoint jobs in production
prod_cursor = jobs_prod.find(
    {"jobType": "pinpoint", "isActive": True}, {"_id": 1, "sourceKey": 1}
)

deactivated = 0
for job in prod_cursor:
    if job["sourceKey"] not in active_keys:
        jobs_prod.update_one(
            {"_id": job["_id"]},
            {
                "$set": {
                    "isActive": False,
                    "updatedAt": datetime.utcnow(),
                    "updatedBy": "pinpoint_inactive_sync",
                }
            },
        )
        deactivated += 1

print(f"🏁 Done. Marked {deactivated} old jobs inactive.")
