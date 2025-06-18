import orjson
import logging
import asyncio
from aiolimiter import AsyncLimiter
from helper.config import load_config
from helper.cache import load_cache, save_cache

config = load_config()
tmdb_response_cache = {}
tmdb_limiter = AsyncLimiter(40, 10)

async def tmdb_api_request(
    endpoint_or_url,
    params=None,
    retries=3,
    delay=2,
    backoff_factor=2,
    api_key=None,
    language=None,
    region=None,
    cache=True,
    raw=False,
    session=None,
    **kwargs
):
    import hashlib
    if session is None:
        raise ValueError("An aiohttp session must be passed to tmdb_api_request")

    if endpoint_or_url.startswith("http"):
        url = endpoint_or_url
        query = params or {}
        cache_key = f"{url}:{orjson.dumps(query, option=orjson.OPT_SORT_KEYS).decode()}"
    else:
        if api_key is None:
            api_key = config["tmdb"]["api_key"]
        if language is None:
            language = config["tmdb"].get("language", "en")
        if region is None:
            region = config["tmdb"].get("region", "US")
        url = f"https://api.themoviedb.org/3/{endpoint_or_url}"
        query = {"api_key": api_key}
        if params is None:
            params = {}
        if "language" not in params:
            params["language"] = language
        if "region" not in params:
            params["region"] = region
        query.update(params)
        cache_key = f"{url}:{orjson.dumps(query, option=orjson.OPT_SORT_KEYS).decode()}"

    cache_hash = hashlib.sha256(cache_key.encode()).hexdigest()
    if cache and cache_hash in tmdb_response_cache:
        logging.debug(f"[TMDb] Returning cached response for {url} params: {params}")
        return tmdb_response_cache[cache_hash]

    for attempt in range(1, retries + 1):
        try:
            async with tmdb_limiter:
                async with session.get(url, params=query, **kwargs) as response:
                    if response.status == 200:
                        if raw:
                            data = await response.read()
                        else:
                            data = await response.json()
                        if cache:
                            tmdb_response_cache[cache_hash] = data
                        return data
                    elif response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", delay))
                        logging.warning(f"[TMDb] Rate limited (HTTP 429). Sleeping {retry_after}s before retry...")
                        await asyncio.sleep(retry_after)
                    else:
                        logging.warning(f"[TMDb] Non-200 response {response.status} for {url}")
        except Exception as e:
            logging.warning(f"[TMDb] Attempt {attempt}: Request failed for URL {url}: {e}")
        if attempt < retries:
            sleep_time = delay * (backoff_factor ** (attempt - 1))
            logging.info(f"[TMDb] Retrying in {sleep_time}s... (Attempt {attempt + 1}/{retries})")
            await asyncio.sleep(sleep_time)
    logging.error(f"[TMDb] Failed after {retries} attempts for {url}")
    return None

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