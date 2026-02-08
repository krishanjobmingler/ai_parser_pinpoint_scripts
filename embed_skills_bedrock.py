import os
import time
import json
from tqdm import tqdm
from pymongo import MongoClient
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# -----------------------------
# Configuration
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "jobminglr_staging"
COLLECTION_NAME = "skills"
BATCH_SIZE = 100  # Titan handles 1 input per call; we just batch DB operations
REGION = "us-east-1"
MODEL_ID = "amazon.titan-embed-text-v1"

# -----------------------------
# Initialize clients
# -----------------------------
mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
skills_col = db[COLLECTION_NAME]
bedrock = boto3.client("bedrock-runtime", region_name=REGION)

# -----------------------------
# Fetch skills needing embeddings
# -----------------------------
skills_cursor = skills_col.find(
    {"isDeleted": False, "embedding": {"$exists": False}},
    {"_id": 1, "name": 1, "aliases": 1}
)
total = skills_col.count_documents({"isDeleted": False, "embedding": {"$exists": False}})
print(f"🧠 Skills pending embedding: {total}")

# -----------------------------
# Helper: generate embedding for text
# -----------------------------
def get_embedding(text, retries=3, delay=2):
    for attempt in range(retries):
        try:
            resp = bedrock.invoke_model(
                modelId=MODEL_ID,
                body=json.dumps({"inputText": text}),
                accept="application/json",
                contentType="application/json",
            )
            data = json.loads(resp["body"].read())
            return data["embedding"]
        except (BotoCoreError, ClientError, KeyError, json.JSONDecodeError) as e:
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
                continue
            print(f"❌ Failed to embed '{text}': {e}")
            return None

# -----------------------------
# Process and store embeddings
# -----------------------------
processed = 0
with tqdm(total=total, desc="Embedding skills", unit="skill") as pbar:
    for doc in skills_cursor:
        skill_id = doc["_id"]
        name = doc.get("name")
        aliases = doc.get("aliases", [])

        # Generate one vector per unique string (name + aliases)
        texts = list({name, *aliases})
        vectors = []
        for t in texts:
            emb = get_embedding(t)
            if emb:
                vectors.append(emb)
                time.sleep(0.1)  # small throttle to stay under Bedrock limits

        if not vectors:
            continue

        # Average embeddings across aliases to get a canonical vector
        dim = len(vectors[0])
        avg_vector = [sum(v[i] for v in vectors) / len(vectors) for i in range(dim)]

        # Write back to Mongo
        skills_col.update_one(
            {"_id": skill_id},
            {"$set": {"embedding": avg_vector}}
        )

        processed += 1
        pbar.update(1)

print(f"\n✅ Done. Embedded {processed} skills into MongoDB.")
