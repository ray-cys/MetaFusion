import orjson
import logging
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
from helper.config import load_config
from helper.cache import (
    load_cache, save_cache, load_failed_cache, save_failed_cache
)

config = load_config()
tmdb_cache = load_cache()
failed_cache = load_failed_cache()
tmdb_response_cache = {}

# TMDb rate limit: 40 requests per 10 seconds
tmdb_limiter = AsyncLimiter(40, 10)

async def safe_get_with_retries(url, params=None, retries=None, backoff_factor=None, cache=True, **kwargs):
    import hashlib
    params_bytes = orjson.dumps(params or {}, option=orjson.OPT_SORT_KEYS)
    cache_key = f"{url}:{params_bytes.decode()}"
    cache_hash = hashlib.sha256(cache_key.encode()).hexdigest()

    if cache and cache_hash in tmdb_response_cache:
        logging.debug(f"[API Cache] Returning cached response for URL: {url} params: {params}")
        return tmdb_response_cache[cache_hash]

    backoff = backoff_factor if backoff_factor is not None else 1
    max_retries = retries if retries is not None else 3
    timeout = aiohttp.ClientTimeout(total=20)

    for attempt in range(1, max_retries + 1):
        try:
            async with tmdb_limiter:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, params=params, **kwargs) as response:
                        if response.status == 200:
                            data = await response.read()
                            if cache:
                                tmdb_response_cache[cache_hash] = data
                            return data
                        elif response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", backoff))
                            logging.warning(f"[API Cache] Rate limited (HTTP 429). Sleeping {retry_after}s before retry...")
                            await asyncio.sleep(retry_after)
                        else:
                            logging.warning(f"[API Cache] Attempt {attempt}: Status {response.status} for URL: {url}")
        except Exception as e:
            logging.warning(f"[API Cache] Attempt {attempt}: Request failed for URL {url}: {e}")
        await asyncio.sleep(backoff * attempt)
    logging.error(f"[API Cache] All retries failed for URL: {url}")
    return None

async def tmdb_api_request(endpoint, params=None, retries=3, delay=2, backoff_factor=2, api_key=None, language=None, region=None):
    import hashlib
    """
    Make an async TMDb API request with retries and exponential backoff.
    """
    logging.debug(f"[TMDb] Requesting {endpoint} for {params}")
    if api_key is None:
        api_key = config["tmdb"]["api_key"]
    if language is None:
        language = config["tmdb"].get("language", "en")
    if region is None:
        region = config["tmdb"].get("region", "US")

    url = f"https://api.themoviedb.org/3/{endpoint}"
    query = {"api_key": api_key}
    if params is None:
        params = {}
    if "language" not in params:
        params["language"] = language
    if "region" not in params:
        params["region"] = region
    query.update(params)

    # Smarter cache key (hash for large params)
    cache_key = f"{url}:{orjson.dumps(query, option=orjson.OPT_SORT_KEYS).decode()}"
    cache_hash = hashlib.sha256(cache_key.encode()).hexdigest()
    if cache_hash in tmdb_response_cache:
        logging.debug(f"[TMDb API] Returning in-memory cached response for {url} params: {params}")
        return tmdb_response_cache[cache_hash]

    timeout = aiohttp.ClientTimeout(total=20)
    for attempt in range(1, retries + 1):
        try:
            async with tmdb_limiter:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, params=query) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data:
                                tmdb_response_cache[cache_hash] = data
                                logging.debug(f"[TMDb API Response] URL: {response.url}")
                                return data
                            else:
                                logging.warning(f"[TMDb API] Empty JSON response (Attempt {attempt}) for {url}")
                        elif response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", delay))
                            logging.warning(f"[TMDb API] Rate limited (HTTP 429). Sleeping {retry_after}s before retry...")
                            await asyncio.sleep(retry_after)
                        else:
                            logging.warning(f"[TMDb API] Non-200 response {response.status} for {url}")
        except Exception as e:
            logging.error(f"[TMDb API] Request exception (Attempt {attempt}): {e}")
        if attempt < retries:
            sleep_time = delay * (backoff_factor ** (attempt - 1))
            logging.info(f"[TMDb API] Retrying in {sleep_time}s... (Attempt {attempt + 1}/{retries})")
            await asyncio.sleep(sleep_time)
    logging.error(f"[TMDb API] Failed after {retries} attempts for {url}")
    logging.debug(f"[TMDb] Finished {endpoint} for {params}")
    return None

async def tmdb_find_id(title, year, media_type):
    """
    Search TMDb for a title and year, returning the best-matched TMDb ID.
    """
    logging.debug(f"[TMDb Search] Searching for Title: {title}, Year: {year}, Type: {media_type}")
    endpoint = f"search/{media_type}"
    params = {
        "query": title,
        "language": config["tmdb"].get("language", "en"),
        "include_adult": False
    }
    if year:
        params["year"] = year
    data = await tmdb_api_request(endpoint, params=params)
    if data and data.get("results"):
        # Sort by vote_count and popularity to get the best match
        best_result = sorted(
            data["results"],
            key=lambda x: (x.get("vote_count", 0), x.get("popularity", 0)),
            reverse=True
        )[0]
        tmdb_id = best_result.get("id")
        logging.debug(f"[TMDb API] TMDb ID found: {tmdb_id} for {title} ({year})")
        return tmdb_id
    logging.debug(f"[TMDb API] No results found for {title} ({year})")
    return None

async def resolve_tmdb_id(item, title, year, media_type):
    import re
    """
    Resolve the TMDb ID for a given item, using cache, Plex GUIDs, or TMDb search.
    """
    # Standardize media_type
    media_type = media_type.lower()
    if media_type == "show":
        media_type = "tv"
    elif media_type != "movie" and media_type != "tv":
        media_type = "movie"
    cache_key = f"{media_type}:{title}:{year}"
    if cache_key in tmdb_cache:
        cache_entry = tmdb_cache[cache_key]
        if isinstance(cache_entry, dict):
            logging.debug(f"[TMDb Resolution] TMDb ID found in cache for {title} ({year}): {cache_entry.get('tmdb_id')}")
            return cache_entry.get("tmdb_id")
        else:
            logging.debug(f"[TMDb Resolution] TMDb ID found in cache for {title} ({year}): {cache_entry}")
            return cache_entry
    if cache_key in failed_cache:
        logging.warning(f"[TMDb Resolution] Skipping repeated failed lookup for {title} ({year})")
        return None

    tmdb_id = None
    # Try to extract from Plex GUIDs
    if item:
        for guid in getattr(item, "guids", []):
            guid_id = guid.id
            if "tmdb" in guid_id:
                tmdb_id = guid_id.split("://")[1].split("?")[0]
                logging.debug(f"[TMDb Resolution] TMDb ID extracted directly from Plex GUID: {tmdb_id}")
                break

    if tmdb_id:
        tmdb_cache[cache_key] = {"tmdb_id": tmdb_id}
        save_cache(tmdb_cache)
        return tmdb_id

    # Fallback: search by title/year and cleaned title
    search_attempts = [(title, year), (title, None)]
    cleaned_title = re.sub(r'\s*\(.*?\)', '', title).strip()
    if cleaned_title != title:
        search_attempts.extend([(cleaned_title, year), (cleaned_title, None)])

    for search_title, search_year in search_attempts:
        tmdb_id = await tmdb_find_id(search_title, search_year, media_type)
        if tmdb_id:
            break

    if tmdb_id:
        tmdb_cache[cache_key] = {"tmdb_id": tmdb_id}
        save_cache(tmdb_cache)
        logging.debug(f"[TMDb Resolution] TMDb ID resolved and cached for {title} ({year}): {tmdb_id}")
    else:
        failed_cache[cache_key] = True
        save_failed_cache(failed_cache)
        logging.warning(f"[TMDb Resolution] Could not resolve TMDb ID for {title} ({year}) after all attempts.")

    return tmdb_id

async def download_poster(image_path, save_path, item=None):
    from helper.plex import safe_title_year
    from modules.assets import save_poster
    """
    Download a poster image from TMDb and save it to the specified path.
    """
    try:
        url = f"https://image.tmdb.org/t/p/original{image_path}"
        logging.debug(f"[Assets Download] Downloading {safe_title_year(item)} poster from URL: {url}")

        # Skip download if file already exists and not in dry run mode
        if save_path.exists() and not config.get("dry_run", False):
            logging.info(f"[Assets Download] Poster {safe_title_year(item)} already exists. Skipping download.")
            return True

        # Dry run mode: simulate download
        if config.get("dry_run", False):
            logging.info(f"[Assets Dry Run] Would download {safe_title_year(item)} from {url}")
            return True 

        response_content = await safe_get_with_retries(url)
        if not response_content:
            logging.warning(f"[Assets Download] Failed to download image for {safe_title_year(item)}")
            return False

        try:
            await save_poster(response_content, save_path, item)
            logging.debug(f"[Assets Download] Downloaded poster for {safe_title_year(item)}")
            return True
        except Exception as e:
            logging.warning(f"[Assets Download] Failed to save poster for {safe_title_year(item)}: {e}")
            return False

    except Exception as e:
        logging.warning(f"[Assets Download] Failed to process poster download for {safe_title_year(item)}: {e}")
        return False

def update_tmdb_cache(cache_key, tmdb_id, title, year, media_type, **kwargs):
    from datetime import datetime
    """
    Update the TMDb cache with a new or updated entry.
    """
    entry = tmdb_cache.get(cache_key, {})
    # Always update these fields
    entry["tmdb_id"] = tmdb_id
    entry["title"] = title
    entry["year"] = year
    entry["media_type"] = media_type
    entry["last_updated"] = datetime.now().isoformat()
    # Optionally update any extra fields passed as kwargs
    for k, v in kwargs.items():
        entry[k] = v
    tmdb_cache[cache_key] = entry
    save_cache(tmdb_cache)