import asyncio, json
from datetime import datetime
from pathlib import Path
from helper.logging import log_cache_event

CACHE_PATH = Path(__file__).parent.parent / "cache"
CACHE_PATH.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_PATH / "meta_cache.json"

def load_cache():
    if CACHE_FILE.exists() and CACHE_FILE.stat().st_size > 0:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
            log_cache_event("cache_loaded", count=len(cache), cache_file=CACHE_FILE)
            return cache
    log_cache_event("cache_empty", cache_file=CACHE_FILE)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    log_cache_event("cache_saved", count=len(cache), cache_file=CACHE_FILE)
    for entry in cache.values():
        if entry.get("media_type") == "tv":
            entry.pop("season_average", None)
            entry.pop("season_number", None)

cache_lock = asyncio.Lock()
async def meta_cache_async(cache_key, tmdb_id, title, year, media_type, update_timestamp=True, **kwargs):
    async with cache_lock:
        cache = load_cache()
        entry = cache.get(cache_key, {})
        entry["tmdb_id"] = tmdb_id
        entry["title"] = title
        entry["year"] = year
        entry["media_type"] = media_type
        if update_timestamp:
            entry["last_updated"] = datetime.now().isoformat()
        season_number = kwargs.pop("season_number", None)
        if season_number is not None:
            seasons = entry.setdefault("seasons", {})
            season_entry = seasons.setdefault(str(season_number), {})
            for k, v in kwargs.items():
                season_entry[k] = v
        else:
            for k, v in kwargs.items():
                entry[k] = v
        cache[cache_key] = entry
        log_cache_event("cache_updated", cache_key=cache_key, media_type=media_type, title=title, year=year)
        save_cache(cache)