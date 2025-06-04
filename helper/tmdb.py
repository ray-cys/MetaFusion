import os
import orjson
import logging
import time
import requests
from pathlib import Path
from threading import RLock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from helper.config import load_config

config = load_config()
cache_lock = RLock()

if os.environ.get("DOCKER_ENV", "0") == "1":
    CACHE_PATH = Path.cwd() / "cache"
else:
    CACHE_PATH = Path(__file__).parent.parent / "cache"
CACHE_PATH.mkdir(exist_ok=True)
CACHE_FILE = CACHE_PATH / "tmdb_cache.json"
FAILED_CACHE_FILE = CACHE_PATH / "failed_items.json"

def load_cache():
    """
    Load the TMDb cache from disk.
    """
    if CACHE_FILE.exists() and CACHE_FILE.stat().st_size > 0:
        with open(CACHE_FILE, "rb") as f:
            return orjson.loads(f.read())
    return {}

def save_cache(cache):
    """
    Save the TMDb cache to disk.
    """
    with open(CACHE_FILE, "wb") as f:
        f.write(orjson.dumps(cache, option=orjson.OPT_INDENT_2))

def load_failed_cache():
    """
    Load the failed TMDb lookups cache from disk.
    """
    if FAILED_CACHE_FILE.exists() and FAILED_CACHE_FILE.stat().st_size > 0:
        with open(FAILED_CACHE_FILE, "rb") as f:
            return orjson.loads(f.read())
    return {}

def save_failed_cache(failed_items):
    """
    Save the failed TMDb lookups cache to disk.
    """
    with open(FAILED_CACHE_FILE, "wb") as f:
        f.write(orjson.dumps(failed_items, option=orjson.OPT_INDENT_2))

tmdb_cache = load_cache()
failed_cache = load_failed_cache()
tmdb_response_cache = {}

# Set up a requests session with retry and connection pooling
tmdb_session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
    max_retries=Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
)
tmdb_session.mount("http://", adapter)
tmdb_session.mount("https://", adapter)

def safe_get_with_retries(url, params=None, retries=None, backoff_factor=None, cache=True, **kwargs):
    import hashlib
    """
    Make a GET request with retries and optional in-memory response caching.
    """
    # Create a unique cache key based on URL and params
    params_bytes = orjson.dumps(params or {}, option=orjson.OPT_SORT_KEYS)
    cache_key = f"{url}:{params_bytes.decode()}"
    cache_hash = hashlib.sha256(cache_key.encode()).hexdigest()

    # Check in-memory cache first
    if cache and cache_hash in tmdb_response_cache:
        logging.debug(f"[API Cache] Returning cached response for URL: {url} params: {params}")
        return tmdb_response_cache[cache_hash]

    backoff = backoff_factor if backoff_factor is not None else 1
    max_retries = retries if retries is not None else 3
    timeout = 10

    for attempt in range(1, max_retries + 1):
        try:
            response = tmdb_session.get(url, params=params, timeout=timeout, **kwargs)
            if response.ok:
                if cache:
                    tmdb_response_cache[cache_hash] = response
                return response
            logging.warning(f"[API Cache] Attempt {attempt}: Status {response.status_code} for URL: {url}")
        except Exception as e:
            logging.warning(f"[API Cache] Attempt {attempt}: Request failed for URL {url}: {e}")
        time.sleep(backoff * attempt)
    logging.error(f"[API Cache] All retries failed for URL: {url}")
    return None

def tmdb_api_request(endpoint, params=None, retries=3, delay=2, backoff_factor=2, api_key=None, language=None, region=None):
    import hashlib
    """
    Make a TMDb API request with retries and exponential backoff.
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

    for attempt in range(1, retries + 1):
        try:
            response = tmdb_session.get(url, params=query, timeout=20)
            if response.status_code == 200:
                data = response.json()
                if data:
                    tmdb_response_cache[cache_hash] = data
                    logging.debug(f"[TMDb API Response] URL: {response.url}")
                    return data
                else:
                    logging.warning(f"[TMDb API] Empty JSON response (Attempt {attempt}) for {url}")
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", delay))
                logging.warning(f"[TMDb API] Rate limited (HTTP 429). Sleeping {retry_after}s before retry...")
                time.sleep(retry_after)
            else:
                logging.warning(f"[TMDb API] Non-200 response {response.status_code} for {url}")
        except requests.exceptions.RequestException as e:
            logging.error(f"[TMDb API] Request exception (Attempt {attempt}): {e}")
        if attempt < retries:
            sleep_time = delay * (backoff_factor ** (attempt - 1))
            logging.info(f"[TMDb API] Retrying in {sleep_time}s... (Attempt {attempt + 1}/{retries})")
            time.sleep(sleep_time)
    logging.error(f"[TMDb API] Failed after {retries} attempts for {url}")
    logging.debug(f"[TMDb] Finished {endpoint} for {params}")
    return None

def tmdb_find_id(title, year, media_type):
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
    data = tmdb_api_request(endpoint, params=params)
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

def resolve_tmdb_id(item, title, year, media_type):
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
    with cache_lock:
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
        with cache_lock:
            tmdb_cache[cache_key] = {"tmdb_id": tmdb_id}
            save_cache(tmdb_cache)
        return tmdb_id

    # Fallback: search by title/year and cleaned title
    search_attempts = [(title, year), (title, None)]
    cleaned_title = re.sub(r'\s*\(.*?\)', '', title).strip()
    if cleaned_title != title:
        search_attempts.extend([(cleaned_title, year), (cleaned_title, None)])

    for search_title, search_year in search_attempts:
        tmdb_id = tmdb_find_id(search_title, search_year, media_type)
        if tmdb_id:
            break

    if tmdb_id:
        with cache_lock:
            tmdb_cache[cache_key] = {"tmdb_id": tmdb_id}
            save_cache(tmdb_cache)
        logging.debug(f"[TMDb Resolution] TMDb ID resolved and cached for {title} ({year}): {tmdb_id}")
    else:
        with cache_lock:
            failed_cache[cache_key] = True
            save_failed_cache(failed_cache)
        logging.warning(f"[TMDb Resolution] Could not resolve TMDb ID for {title} ({year}) after all attempts.")

    return tmdb_id

def update_tmdb_cache(cache_key, tmdb_id, title, year, media_type, **kwargs):
    from datetime import datetime
    """
    Update the TMDb cache with a new or updated entry.
    """
    with cache_lock:
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