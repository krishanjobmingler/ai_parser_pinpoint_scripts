#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "[$(date)] Starting Pinpoint full pipeline"
echo "Scripts directory: $SCRIPT_DIR"
echo "============================================"

if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
elif [ -f "$SCRIPT_DIR/../venv/bin/activate" ]; then
    source "$SCRIPT_DIR/../venv/bin/activate"
fi

echo "Fetching new Pinpoint jobs..."
python3 "$SCRIPT_DIR/pinpoint_fetch.py"

echo "Parsing job descriptions..."
python3 "$SCRIPT_DIR/pinpoint_parse_ai.py"

echo "Canonical mapping..."
python3 "$SCRIPT_DIR/canonical_mapper.py"

echo "Auto-validating all ready jobs..."
mongosh "mongodb://localhost:27017/jobminglr_staging" --eval '
db.jobs.updateMany(
  { validated: { $ne: true } },
  { $set: { validated: true }, $unset: { synced: "" } }
)
'

echo "Syncing validated jobs to production..."
python3 "$SCRIPT_DIR/production_sync_batch.py"

echo "[$(date)] Pipeline complete."
echo "Logs: $SCRIPT_DIR/logs/$(date +%Y-%m-%d)/"
echo "============================================"
