import os
import orjson
import logging
from pathlib import Path

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
            logging.debug(f"[Cache] Loaded {len(cache)} entries from {CACHE_FILE}")
            return cache
    logging.debug(f"[Cache] No cache file found at {CACHE_FILE}, starting with empty cache.")
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        f.write(orjson.dumps(cache, option=orjson.OPT_INDENT_2))
    logging.debug(f"[Cache] Saved {len(cache)} entries to {CACHE_FILE}")

def meta_cache(cache_key, tmdb_id, title, year, media_type, **kwargs):
    from datetime import datetime
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
    logging.debug(f"[Cache] Updated cache for key '{cache_key}' ({media_type}): {title} ({year})")
    save_cache(cache)