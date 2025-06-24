import os
import logging
import orjson
import asyncio
from datetime import datetime
from pathlib import Path
from helper.logging import log_helper_event

if os.environ.get("DOCKER_ENV", "0") == "1":
    CACHE_PATH = Path.cwd() / "cache"
else:
    CACHE_PATH = Path(__file__).parent.parent / "cache"
CACHE_PATH.mkdir(exist_ok=True)
CACHE_FILE = CACHE_PATH / "meta_cache.json"

def load_cache():
    if CACHE_FILE.exists() and CACHE_FILE.stat().st_size > 0:
        with open(CACHE_FILE, "rb") as f:
            cache = orjson.loads(f.read())
            log_helper_event("cache_loaded", count=len(cache), cache_file=CACHE_FILE)
            return cache
    log_helper_event("cache_empty", cache_file=CACHE_FILE)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        f.write(orjson.dumps(cache, option=orjson.OPT_INDENT_2))
    log_helper_event("cache_saved", count=len(cache), cache_file=CACHE_FILE)

cache_lock = asyncio.Lock()
async def meta_cache_async(cache_key, tmdb_id, title, year, media_type, **kwargs):
    cache = load_cache() 
    entry = cache.get(cache_key, {})
    entry["tmdb_id"] = tmdb_id
    entry["title"] = title
    entry["year"] = year
    entry["media_type"] = media_type
    entry["last_updated"] = datetime.now().isoformat()
    for k, v in kwargs.items():
        entry[k] = v
    cache[cache_key] = entry
    log_helper_event("cache_updated", cache_key=cache_key, media_type=media_type, title=title, year=year)
    save_cache(cache)