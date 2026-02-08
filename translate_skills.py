#!/usr/bin/env python3

import sys
import asyncio
import re
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from deep_translator import GoogleTranslator

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"

BATCH_SIZE = 100
LOG_FILE = "/home/krishan/Fee_projects/Job_mingler/code/scripts/translate_skills_log.txt"

translator = GoogleTranslator(source='auto', target='en')

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def is_english(text: str) -> bool:
    """Check if text is primarily English."""
    if not text:
        return True
    # Check if text contains non-ASCII characters (common in non-English)
    non_ascii = sum(1 for c in text if ord(c) > 127)
    # If more than 20% non-ASCII, likely not English
    if non_ascii / len(text) > 0.2:
        return False
    # Check for common non-English patterns
    non_english_patterns = [
        r'[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]',  # Accented chars
        r'[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]',  # Polish
        r'[абвгдеёжзийклмнопрстуфхцчшщъыьэюя]',  # Cyrillic
        r'[αβγδεζηθικλμνξοπρστυφχψω]',  # Greek
        r'[\u4e00-\u9fff]',  # Chinese
        r'[\u3040-\u309f\u30a0-\u30ff]',  # Japanese
        r'[\uac00-\ud7af]',  # Korean
    ]
    for pattern in non_english_patterns:
        if re.search(pattern, text.lower()):
            return False
    return True

async def translate_text(text: str) -> str:
    """Translate text to English (async)."""
    try:
        if not text or len(text) < 2:
            return text
        translated = await asyncio.to_thread(translator.translate, text)
        return translated if translated else text
    except Exception as e:
        return text  # Return original on error

async def translate_batch(batch: list) -> list:
    """Translate a batch of skills in parallel."""
    tasks = [translate_text(skill["name"]) for skill in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    translated_items = []
    for skill, result in zip(batch, results):
        if isinstance(result, Exception):
            result = skill["name"]  # Keep original on error
        translated_items.append({
            "_id": skill["_id"],
            "original": skill["name"],
            "translated": result
        })
    return translated_items

async def translate_skills(limit: int = 1000, dry_run: bool = True):
    start_time = datetime.now()
    
    log("=" * 60)
    log(f"🌍 TRANSLATE SKILLS TO ENGLISH {'(DRY RUN)' if dry_run else '(LIVE MODE)'}")
    log(f"⏱️  Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"📋 Limit: {limit} | Batch: {BATCH_SIZE}")
    log("=" * 60)
    
    client = AsyncIOMotorClient(PROD_URI)
    db = client[PROD_DB]
    skills_col = db["New_skills"]
    
    total_in_db = await skills_col.count_documents({})
    log(f"📊 Total skills in DB: {total_in_db}")
    
    # Fetch skills
    cursor = skills_col.find(
        {},
        {"_id": 1, "name": 1}
    ).sort("createdAt", -1)
    
    if limit > 0:
        cursor = cursor.limit(limit)
    
    skills_list = await cursor.to_list(length=limit if limit > 0 else None)
    total_skills = len(skills_list)
    log(f"📊 Skills to check: {total_skills}")
    
    # Find non-English skills
    non_english = []
    for skill in skills_list:
        name = skill.get("name", "")
        if not is_english(name):
            non_english.append(skill)
    
    log(f"🌍 Non-English skills found: {len(non_english)}")
    
    if not non_english:
        log("✅ All skills are already in English!")
        client.close()
        return
    
    total_translated = 0
    total_skipped = 0
    
    # Process in batches (parallel)
    for batch_start in range(0, len(non_english), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(non_english))
        batch = non_english[batch_start:batch_end]
        
        progress = (batch_end / len(non_english)) * 100
        log(f"\n[{batch_start + 1}-{batch_end}/{len(non_english)}] ({progress:.1f}%) Translating {len(batch)} in parallel...")
        
        # Translate all in parallel
        translated_items = await translate_batch(batch)
        
        # Process results and update DB
        update_tasks = []
        now = datetime.now(timezone.utc)
        
        for item in translated_items:
            original = item["original"]
            translated = item["translated"]
            
            if translated and translated.lower() != original.lower():
                log(f"   🌍 '{original}' → '{translated}'")
                
                if not dry_run:
                    update_tasks.append(
                        skills_col.update_one(
                            {"_id": item["_id"]},
                            {"$set": {
                                "name": translated.lower(),
                                "aliases": [translated.lower(), original.lower()],
                                "updatedAt": now
                            }}
                        )
                    )
                total_translated += 1
            else:
                log(f"   ⏭️ '{original}' (no change)")
                total_skipped += 1
        
        # Execute all DB updates in parallel
        if update_tasks:
            await asyncio.gather(*update_tasks)
            log(f"   💾 Updated {len(update_tasks)} skills")
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.5)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    log("\n" + "=" * 60)
    log("📊 SUMMARY")
    log("=" * 60)
    log(f"📋 Skills checked: {total_skills}")
    log(f"🌍 Non-English found: {len(non_english)}")
    log(f"✅ {'Would translate' if dry_run else 'Translated'}: {total_translated}")
    log(f"⏭️ Skipped: {total_skipped}")
    log(f"⏱️  Duration: {duration:.2f}s")
    log("=" * 60)
    
    if dry_run:
        log("⚠️  DRY RUN. Use --apply to make changes.")
    
    client.close()

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    dry_run = "--apply" not in sys.argv
    
    print(f"🌍 Translate Skills to English")
    print(f"   Limit: {limit if limit > 0 else 'ALL'}")
    print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE - APPLYING CHANGES'}")
    print()
    
    asyncio.run(translate_skills(limit=limit, dry_run=dry_run))

