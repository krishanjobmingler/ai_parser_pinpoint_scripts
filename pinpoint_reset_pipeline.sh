#!/bin/bash
set -e

echo ""
echo "======================================"
echo "💣 FULL PIPELINE RESET (Local Only)"
echo "======================================"
echo ""

# --- CONFIG ---
LOCAL_URI="mongodb://localhost:27017"
LOCAL_DB="jobminglr_staging"

# --- 1️⃣ CLEAR LOCAL DATABASE COMPLETELY ---
echo "🧹 Dropping all local collections in ${LOCAL_DB} ..."
mongosh "${LOCAL_URI}" --quiet --eval "
use ${LOCAL_DB};
for (const c of db.getCollectionNames()) {
  db[c].drop();
  print('   🗑️ Dropped ' + c);
}
"
echo "✅ Local MongoDB cleared."

echo ""
echo "======================================"
echo "🏁 RESET COMPLETE"
echo "======================================"
echo ""
echo "Note: Production data (skills, job-titles, jobs) is NOT affected."
echo "      canonical_mapper.py creates new entries directly in LIVE DB."
echo "======================================"
