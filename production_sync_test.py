import os
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.expanduser("~/pinpoint/.env"))

# --- Local (staging) ---
LOCAL_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
LOCAL_DB = os.getenv("MONGO_DB", "jobminglr_staging")

# --- Production ---
PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/jobminglr?retryWrites=true&w=majority"
PROD_DB = "jobminglr"

# Connect to both
local = MongoClient(LOCAL_URI)[LOCAL_DB]
prod = MongoClient(PROD_URI)[PROD_DB]

print("🌐 Connected to:")
print(" - Local:", LOCAL_URI)
print(" - Production:", PROD_URI)
print()

job_id_input = input("Enter the _id of the job to sync: ").strip()
try:
    job_id = ObjectId(job_id_input)
except Exception:
    raise ValueError("❌ Invalid ObjectId format")

job = local["pinpoint_jobs_ready"].find_one({"_id": job_id})
if not job:
    raise ValueError("❌ No job found with that _id in pinpoint_jobs_ready")

# Transform job for production schema
prod_job = {
    "absoluteUrl": job.get("absoluteUrl"),
    "address": job.get("address"),
    "benefits": job.get("benefits", []),
    "company": job.get("company"),
    "companyCulture": job.get("companyCulture", []),
    "createdAt": datetime.utcnow(),
    "createdBy": "system",
    "employmentType": job.get("employmentType"),
    "experienceLevel": job.get("experienceLevel"),
    "goodToHaveSkill": job.get("goodToHaveSkill", []),
    "isActive": True,
    "isAddedFromPinpoint": True,
    "isDeleted": False,
    "isSalaryDisclosed": job.get("isSalaryDisclosed", False),
    "jobDescription": job.get("jobDescriptionClean"),
    "jobInitDate": datetime.utcnow(),
    "jobTitleForFrontend": job.get("jobTitleForFrontend"),
    "jobType": "pinpoint",
    "location": job.get("location", {"type": "Point", "coordinates": [0, 0]}),
    "maxSalary": job.get("maxSalary", 0),
    "minSalary": job.get("minSalary", 0),
    "qualifications": job.get("qualifications", []),
    "rate": job.get("rate", ""),
    "skillRequired": job.get("skillRequired", []),
    "updatedAt": datetime.utcnow(),
    "updatedBy": "system",
    "workArrangement": job.get("workArrangement", ""),
    "sourceKey": job.get("sourceKey"),
}

print(f"🧾 Preparing to sync: {prod_job['jobTitleForFrontend']} ({job['_id']})")

# Insert into production
res = prod["jobs"].insert_one(prod_job)
print(f"✅ Inserted into production as _id: {res.inserted_id}")

# Mark local job as synced
local["pinpoint_jobs_ready"].update_one(
    {"_id": job["_id"]},
    {"$set": {"synced": True, "syncedAt": datetime.utcnow()}}
)
print("🟢 Local job marked as synced.")
