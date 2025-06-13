import datetime
import os

def should_run_now(config=None):
    """
    Determines if the script should run now, based on schedule in config or environment variables.
    Supports multiple run_times (["HH:MM", ...]) for precise scheduling.
    """
    frequency = None
    schedule = {}
    run_times = ["00:00"]  # Default to midnight

    if config and "upgrade_schedule" in config:
        schedule = config.get("upgrade_schedule", {})
        frequency = schedule.get("frequency", "daily")
        run_times = schedule.get("run_times", ["00:00"])
        # Support legacy single run_time
        if isinstance(run_times, str):
            run_times = [run_times]
    else:
        frequency = os.environ.get("SCHEDULE_FREQUENCY", "daily").lower()
        env_run_times = os.environ.get("SCHEDULE_RUN_TIMES", "00:00")
        run_times = [t.strip() for t in env_run_times.split(",")]

    now = datetime.datetime.now()
    current_time = f"{now.hour:02d}:{now.minute:02d}"

    def match_time():
        return current_time in run_times

    if frequency == "always":
        return True
    if frequency == "daily":
        return match_time()
    elif frequency == "twice_a_week":
        days = schedule.get("days", [0, 3])  # 0=Monday, 3=Thursday
        return now.weekday() in days and match_time()
    elif frequency == "weekly":
        day = schedule.get("day", 0)
        return now.weekday() == day and match_time()
    elif frequency == "monthly":
        return now.day == 1 and match_time()
    elif frequency == "custom":
        return True
    return False