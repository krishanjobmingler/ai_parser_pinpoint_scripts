#!/bin/bash
# ---------------------------------------------------------------------------
# DEPRECATED: No longer needed
# Skills and Job Titles are now created directly in LIVE DB by canonical_mapper.py
# ---------------------------------------------------------------------------

echo ""
echo "======================================"
echo "⚠️  DEPRECATED: Taxonomy sync no longer needed"
echo "======================================"
echo ""
echo "Skills and Job Titles are now created directly in LIVE database"
echo "by canonical_mapper.py when not found."
echo ""

# --- CONFIG ---
# LOCAL_DB="jobminglr_staging"
# PROD_URI="mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net"
# PROD_DB="jobminglr"
# BACKUP_DIR="/home/ubuntu/pinpoint/mongo_backup/prod_taxonomy"

# --- 1️⃣ CLEAN PREVIOUS DUMPS ---
# rm -rf "${BACKUP_DIR}"
# mkdir -p "${BACKUP_DIR}"

# --- 2️⃣ DUMP FROM PRODUCTION ---
# echo "📦 Dumping skills + job-titles from production..."
# mongodump \
#   --uri="${PROD_URI}/${PROD_DB}" \
#   --collection=skills \
#   --out="${BACKUP_DIR}"

# mongodump \
#   --uri="${PROD_URI}/${PROD_DB}" \
#   --collection=job-titles \
#   --out="${BACKUP_DIR}"

# echo "✅ Production taxonomy dumped."

# --- 3️⃣ RESTORE INTO LOCAL STAGING ---
# echo ""
# echo "🚚 Restoring skills + job-titles into local DB..."
# mongorestore \
#   --uri="mongodb://localhost:27017/${LOCAL_DB}" \
#   --dir="${BACKUP_DIR}/${PROD_DB}" \
#   --nsFrom="${PROD_DB}.*" \
#   --nsTo="${LOCAL_DB}.*" \
#   --drop

# --- 4️⃣ VERIFY COUNTS ---
# echo ""
# mongosh "mongodb://localhost:27017/${LOCAL_DB}" --quiet --eval '
# print("skills:", db.skills.countDocuments());
# print("job-titles:", db["job-titles"].countDocuments());
# '

echo ""
echo "======================================"
echo "🏁 SCRIPT SKIPPED (code commented out)"
echo "======================================"
