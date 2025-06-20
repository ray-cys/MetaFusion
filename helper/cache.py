import os
import orjson
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
            return orjson.loads(f.read())
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        f.write(orjson.dumps(cache, option=orjson.OPT_INDENT_2))

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
    save_cache(cache)