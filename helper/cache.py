import os
import orjson
from pathlib import Path

if os.environ.get("DOCKER_ENV", "0") == "1":
    CACHE_PATH = Path.cwd() / "cache"
else:
    CACHE_PATH = Path(__file__).parent.parent / "cache"
CACHE_PATH.mkdir(exist_ok=True)
CACHE_FILE = CACHE_PATH / "meta_cache.json"
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