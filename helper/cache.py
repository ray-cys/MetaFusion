import asyncio, json
from datetime import datetime
from helper.config import CACHE_DIR
from helper.logging import log_cache_event

CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "meta_cache.json"

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
async def meta_cache_async(
    cache_key, tmdb_id, title, year, media_type, update_timestamp=True, asset_upgraded=False, 
    poster_upgraded=False, background_upgraded=False, season_upgraded=None, **kwargs
):
    async with cache_lock:
        cache = load_cache()
        entry = cache.get(cache_key, {})
        entry["tmdb_id"] = tmdb_id
        entry["title"] = title
        entry["year"] = year
        entry["media_type"] = media_type
        now_iso = datetime.now().isoformat()
        if update_timestamp:
            entry["last_updated"] = now_iso
        if asset_upgraded:
            entry["asset_last_upgraded"] = now_iso
        if poster_upgraded:
            entry["poster_last_upgraded"] = now_iso
        if background_upgraded:
            entry["background_last_upgraded"] = now_iso
        season_number = kwargs.pop("season_number", None)
        if season_number is not None:
            seasons = entry.setdefault("seasons", {})
            season_entry = seasons.setdefault(str(season_number), {})
            for k, v in kwargs.items():
                season_entry[k] = v
            if isinstance(season_upgraded, int) and season_upgraded == int(season_number):
                season_entry["season_last_upgraded"] = now_iso
        else:
            for k, v in kwargs.items():
                entry[k] = v
        cache[cache_key] = entry
        log_cache_event("cache_updated", cache_key=cache_key, media_type=media_type, title=title, year=year)
        save_cache(cache)