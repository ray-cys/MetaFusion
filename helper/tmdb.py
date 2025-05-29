import json
import logging
import re
import time
from pathlib import Path
from threading import Lock
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from helper.config import load_config

config = load_config()

cache_lock = Lock()
CACHE_DIR = Path(__file__).parent.parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / config.get("cache_file", "tmdb_cache.json")
FAILED_CACHE_FILE = CACHE_DIR / "failed_items.json"

def load_cache():
    """
    Load the TMDb cache from disk.

    Returns:
        dict: The loaded cache dictionary, or an empty dict if not found.
    """
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    """
    Save the TMDb cache to disk.

    Args:
        cache (dict): The cache dictionary to save.
    """
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4, ensure_ascii=False)

def load_failed_cache():
    """
    Load the failed TMDb lookups cache from disk.

    Returns:
        dict: The loaded failed cache dictionary, or an empty dict if not found.
    """
    if FAILED_CACHE_FILE.exists():
        with open(FAILED_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_failed_cache(failed_items):
    """
    Save the failed TMDb lookups cache to disk.

    Args:
        failed_items (dict): The failed cache dictionary to save.
    """
    with open(FAILED_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(failed_items, f, indent=4, ensure_ascii=False)

tmdb_cache = load_cache()
failed_cache = load_failed_cache()
tmdb_response_cache = {}

# Set up a requests session with retry and connection pooling
tmdb_session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=config["network"].get("pool_connections", 100),
    pool_maxsize=config["network"].get("pool_maxsize", 100),
    max_retries=Retry(
        total=config["network"].get("max_retries", 3),
        backoff_factor=config["network"].get("backoff_factor", 1),
        status_forcelist=[500, 502, 503, 504]
    )
)
tmdb_session.mount("http://", adapter)
tmdb_session.mount("https://", adapter)

def safe_get_with_retries(url, params=None, retries=None, backoff_factor=None, cache=True, **kwargs):
    """
    Make a GET request with retries and optional in-memory response caching.

    Args:
        url (str): The URL to request.
        params (dict, optional): Query parameters for the request.
        retries (int, optional): Number of retry attempts.
        backoff_factor (float, optional): Backoff multiplier between retries.
        cache (bool, optional): Whether to use in-memory response cache.
        **kwargs: Additional arguments for requests.get.

    Returns:
        requests.Response or None: The response object if successful, else None.
    """
    backoff = backoff_factor if backoff_factor is not None else config["network"].get("backoff_factor", 1)
    max_retries = retries if retries is not None else config["network"].get("max_retries", 3)
    timeout = config["network"].get("timeout", 10)
    cache_key = f"{url}:{json.dumps(params, sort_keys=True)}"

    # Use in-memory cache if enabled
    if cache and cache_key in tmdb_response_cache:
        logging.debug(f"[API Cache] Returning cached response for URL: {url}")
        return tmdb_response_cache[cache_key]

    for attempt in range(1, max_retries + 1):
        try:
            response = tmdb_session.get(url, params=params, timeout=timeout, **kwargs)
            if response.ok:
                if cache:
                    tmdb_response_cache[cache_key] = response
                return response
            logging.warning(f"[API Cache] Attempt {attempt}: Status {response.status_code} for URL: {url}")
        except Exception as e:
            logging.warning(f"[API Cache] Attempt {attempt}: Request failed for URL {url}: {e}")
        time.sleep(backoff * attempt)
    logging.error(f"[API Cache] All retries failed for URL: {url}")
    return None

def tmdb_api_request(endpoint, params=None, retries=3, delay=2, backoff_factor=2, api_key=None, language=None, region=None):
    """
    Make a TMDb API request with retries and exponential backoff.

    Args:
        endpoint (str): TMDb API endpoint (e.g., "search/movie").
        params (dict, optional): Query parameters.
        retries (int, optional): Number of retry attempts.
        delay (int, optional): Initial delay between retries.
        backoff_factor (float, optional): Exponential backoff multiplier.
        api_key (str, optional): TMDb API key.
        language (str, optional): Language code.
        region (str, optional): Region code.

    Returns:
        dict or None: The JSON response data, or None if all attempts fail.
    """
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

    for attempt in range(1, retries + 1):
        try:
            response = tmdb_session.get(url, params=query, timeout=20)
            if response.status_code == 200:
                data = response.json()
                if data:
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
    return None

def tmdb_find_id(title, year, media_type):
    """
    Search TMDb for a title and year, returning the best-matched TMDb ID.

    Args:
        title (str): The title to search for.
        year (int or str or None): The year of the media.
        media_type (str): "movie" or "tv".

    Returns:
        int or None: The TMDb ID if found, else None.
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
    """
    Resolve the TMDb ID for a given item, using cache, Plex GUIDs, or TMDb search.

    Args:
        item: The Plex item (may be None).
        title (str): The title of the media.
        year (int or str): The year of the media.
        media_type (str): "movie" or "tv".

    Returns:
        int or None: The resolved TMDb ID, or None if not found.
    """
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

def update_tmdb_cache(cache_key, tmdb_id, title, year, media_type):
    """
    Update the TMDb cache with a new or updated entry.

    Args:
        cache_key (str): The cache key for the entry.
        tmdb_id (int): The TMDb ID.
        title (str): The title of the media.
        year (int or str): The year of the media.
        media_type (str): "movie" or "tv".
    """
    with cache_lock:
        tmdb_cache[cache_key] = {
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "media_type": media_type,
            "last_updated": datetime.now().isoformat()
        }
        save_cache(tmdb_cache)