import datetime
from helper.cache import load_cache, save_cache

def get_last_run_time(cache_key="last_metadata_upgrade"):
    cache = load_cache()
    ts = cache.get(cache_key)
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(ts)
    except Exception:
        return None

def set_last_run_time(cache_key="last_metadata_upgrade"):
    cache = load_cache()
    now = datetime.datetime.now().isoformat()
    cache[cache_key] = now
    save_cache(cache)

def should_run_upgrade(config, cache_key="last_metadata_upgrade"):
    schedule = config.get("upgrade_schedule", {})
    frequency = schedule.get("frequency", "daily")
    last_run = get_last_run_time(cache_key)
    now = datetime.datetime.now()

    if frequency == "daily":
        if not last_run or (now - last_run).days >= 1:
            return True
    elif frequency == "twice_a_week":
        days = schedule.get("days", [1, 4])
        if now.weekday() in days:
            if not last_run or last_run.date() != now.date():
                return True
    elif frequency == "weekly":
        if now.weekday() == schedule.get("day", 0):  # Default Monday
            if not last_run or last_run.date() != now.date():
                return True
    elif frequency == "monthly":
        times = schedule.get("times", 1)
        if not last_run or (now - last_run).days >= (30 // times):
            return True
    elif frequency == "custom":
        return True
    return False