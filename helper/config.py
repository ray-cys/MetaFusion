import os, yaml
from pathlib import Path
from helper.logging import log_config_event

def safe_int(val, default, key=None):
    try:
        return int(val)
    except (TypeError, ValueError):
        if key:
            log_config_event("invalid_env_var", key=key, value=val, default=default)
        return default

def safe_float(val, default, key=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        if key:
            log_config_event("invalid_env_var", key=key, value=val, default=default)
        return default
    
BASE_CONFIG_DIR = Path(os.environ.get("CONFIG_DIR", "/config"))
CONFIG_FILE = BASE_CONFIG_DIR / "config.yml"
TEMPLATE_FILE = Path(__file__).parent.parent / "config_template.yml"
CACHE_DIR = BASE_CONFIG_DIR / "cache"

DEFAULT_CONFIG = {
    "metafusion_run": os.environ.get("METAFUSION_RUN", "True").lower() == "true",
    "settings": {
        "schedule": os.environ.get("RUN_SCHEDULE", "True").lower() == "true",
        "run_times": [t.strip() for t in os.environ.get("RUN_TIMES", "06:00,18:30").split(",") if t.strip()],
        "dry_run": os.environ.get("DRY_RUN", "False").lower() == "true",
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
        "mode": os.environ.get("RUN_MODE", "kometa"),
        "path": os.environ.get("KOMETA_PATH", "/kometa/"),
    },
    "plex": {
        "url": os.environ.get("PLEX_URL", "http://10.0.0.1:32400"),
        "token": os.environ.get("PLEX_TOKEN", "PLEX_TOKEN"),
    },
    "plex_libraries": os.environ.get("PLEX_LIBRARIES", "Movies,TV Shows").split(","),
    "tmdb": {
        "api_key": os.environ.get("TMDB_API_KEY", "TMDB_API_KEY"),
        "language": os.environ.get("TMDB_LANGUAGE", "en"),
        "fallback": os.environ.get("TMDB_LANGUAGE_FALLBACK", "zh,ja").split(","),
        "region": os.environ.get("TMDB_REGION", "US"),
    },
    "metadata": {
        "run_basic": os.environ.get("RUN_BASIC", "True").lower() == "true",
        "run_enhanced": os.environ.get("RUN_ENHANCED", "True").lower() == "true",
    },
    "assets": {
        "run_poster": os.environ.get("RUN_POSTER", "True").lower() == "true",
        "run_season": os.environ.get("RUN_SEASON", "True").lower() == "true",
        "run_background": os.environ.get("RUN_BACKGROUND", "False").lower() == "true",
    },
    "cleanup": {
        "run_process": os.environ.get("RUN_PROCESS", "False").lower() == "true"
    },
    "poster_set": {
        "max_width": safe_int(os.environ.get("POSTER_MAX_WIDTH", 2000), 2000, key="POSTER_MAX_WIDTH"),
        "max_height": safe_int(os.environ.get("POSTER_MAX_HEIGHT", 3000), 3000, key="POSTER_MAX_HEIGHT"),
        "min_width": safe_int(os.environ.get("POSTER_MIN_WIDTH", 1000), 1000, key="POSTER_MIN_WIDTH"),
        "min_height": safe_int(os.environ.get("POSTER_MIN_HEIGHT", 1500), 1500, key="POSTER_MIN_HEIGHT"),
        "prefer_vote": safe_float(os.environ.get("POSTER_PREFER_VOTE", 5.0), 5.0, key="POSTER_PREFER_VOTE"),
        "vote_relaxed": safe_float(os.environ.get("POSTER_VOTE_RELAXED", 3.5), 3.5, key="POSTER_VOTE_RELAXED"),
        "vote_threshold": safe_float(os.environ.get("POSTER_VOTE_THRESHOLD", 5.0), 5.0, key="POSTER_VOTE_THRESHOLD"),
    },
    "season_set": {
        "max_width": safe_int(os.environ.get("SEASON_MAX_WIDTH", 2000), 2000, key="SEASON_MAX_WIDTH"),
        "max_height": safe_int(os.environ.get("SEASON_MAX_HEIGHT", 3000), 3000, key="SEASON_MAX_HEIGHT"),
        "min_width": safe_int(os.environ.get("SEASON_MIN_WIDTH", 1000), 1000, key="SEASON_MIN_WIDTH"),
        "min_height": safe_int(os.environ.get("SEASON_MIN_HEIGHT", 1500), 1500, key="SEASON_MIN_HEIGHT"),
        "prefer_vote": safe_float(os.environ.get("SEASON_PREFER_VOTE", 5.0), 5.0, key="SEASON_PREFER_VOTE"),
        "vote_relaxed": safe_float(os.environ.get("SEASON_VOTE_RELAXED", 0.5), 0.5, key="SEASON_VOTE_RELAXED"),
        "vote_threshold": safe_float(os.environ.get("SEASON_VOTE_THRESHOLD", 3.0), 3.0, key="SEASON_VOTE_THRESHOLD"),
    },
    "background_set": {
        "max_width": safe_int(os.environ.get("BG_MAX_WIDTH", 3840), 3840, key="BG_MAX_WIDTH"),
        "max_height": safe_int(os.environ.get("BG_MAX_HEIGHT", 2160), 2160, key="BG_MAX_HEIGHT"),
        "min_width": safe_int(os.environ.get("BG_MIN_WIDTH", 1920), 1920, key="BG_MIN_WIDTH"),
        "min_height": safe_int(os.environ.get("BG_MIN_HEIGHT", 1080), 1080, key="BG_MIN_HEIGHT"),
        "prefer_vote": safe_float(os.environ.get("BG_PREFER_VOTE", 5.0), 5.0, key="BG_PREFER_VOTE"),
        "vote_relaxed": safe_float(os.environ.get("BG_VOTE_RELAXED", 3.5), 3.5, key="BG_VOTE_RELAXED"),
        "vote_threshold": safe_float(os.environ.get("BG_VOTE_THRESHOLD", 5.0), 5.0, key="BG_VOTE_THRESHOLD"),
    },
}

def get_disabled_features(config, logger):
    features = [
        (("metadata", "run_basic"), "Metadata Extraction"),
        (("metadata", "run_enhanced"), "Enhanced Metadata Extraction"),
        (("assets", "run_poster"), "Poster Assets Download"),
        (("assets", "run_season"), "Season Assets Download"),
        (("assets", "run_background"), "Background Assets Download"),
        (("cleanup", "run_process"), "Cleanup Libraries"),
    ]
    for key_tuple, feature in features:
        sub_config = config
        for k in key_tuple:
            sub_config = sub_config.get(k, None)
            if sub_config is None:
                break
        enabled = bool(sub_config)
        event = "feature_enabled" if enabled else "feature_disabled"
        log_config_event(event, feature=feature)

def get_feature_flags(config):
    feature_flags = {
        "dry_run": config.get("settings", {}).get("dry_run", False),
        "metadata_basic": config.get("metadata", {}).get("run_basic", True),
        "metadata_enhanced": config.get("metadata", {}).get("run_enhanced", True),
        "poster": config.get("assets", {}).get("run_poster", True),
        "season": config.get("assets", {}).get("run_season", True),
        "background": config.get("assets", {}).get("run_background", False),
        "cleanup": config.get("cleanup", {}).get("run_process", False),
    }
    return feature_flags

def warn_unknown_keys(user_cfg, default_cfg, parent_key=""):
    for key in user_cfg:
        if key not in default_cfg:
            full_key = f"{parent_key}.{key}" if parent_key else key
            log_config_event("unknown_key", key=full_key)
        elif isinstance(user_cfg[key], dict) and isinstance(default_cfg[key], dict):
            warn_unknown_keys(user_cfg[key], default_cfg[key], parent_key=f"{parent_key}.{key}" if parent_key else key)

def merge_config_dicts(default, user):
    for k, v in user.items():
        if isinstance(v, dict) and isinstance(default.get(k), dict):
            merge_config_dicts(default[k], v)
        else:
            default[k] = v

def mode_check(config, mode="kometa"):
    return config.get("settings", {}).get("mode", "kometa").lower() == mode.lower()

def load_config_file():
    import shutil
    env_vars = [
        "METAFUSION_RUN", "RUN_SCHEDULE", "RUN_TIMES", "DRY_RUN", "LOG_LEVEL", "RUN_MODE", "KOMETA_PATH",
        "PLEX_URL", "PLEX_TOKEN", "PLEX_LIBRARIES",
        "TMDB_API_KEY", "TMDB_LANGUAGE", "TMDB_LANGUAGE_FALLBACK", "TMDB_REGION",
        "RUN_BASIC", "RUN_ENHANCED", "RUN_POSTER", "RUN_SEASON", "RUN_BACKGROUND", "RUN_PROCESS",
        "POSTER_MAX_WIDTH", "POSTER_MAX_HEIGHT", "POSTER_MIN_WIDTH", "POSTER_MIN_HEIGHT",
        "POSTER_PREFER_VOTE", "POSTER_VOTE_RELAXED", "POSTER_VOTE_THRESHOLD",
        "SEASON_MAX_WIDTH", "SEASON_MAX_HEIGHT", "SEASON_MIN_WIDTH", "SEASON_MIN_HEIGHT",
        "SEASON_PREFER_VOTE", "SEASON_VOTE_RELAXED", "SEASON_VOTE_THRESHOLD",
        "BG_MAX_WIDTH", "BG_MAX_HEIGHT", "BG_MIN_WIDTH", "BG_MIN_HEIGHT",
        "BG_PREFER_VOTE", "BG_VOTE_RELAXED", "BG_VOTE_THRESHOLD"
    ]
    all_env_present = all(os.environ.get(var) is not None for var in env_vars)

    if (not all_env_present or not any(os.environ.get(var) is not None for var in env_vars)) and not CONFIG_FILE.exists():
        if TEMPLATE_FILE.exists():
            shutil.copy(TEMPLATE_FILE, CONFIG_FILE)
            log_config_event("yaml_not_found", config_file=CONFIG_FILE)
        else:
            log_config_event("yaml_missing", config_file=CONFIG_FILE)

    config = DEFAULT_CONFIG.copy()
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            try:
                user_config = yaml.safe_load(f) or {}
                warn_unknown_keys(user_config, DEFAULT_CONFIG)
                merge_config_dicts(config, user_config)
                log_config_event("config_loaded", config_file=CONFIG_FILE)
            except yaml.YAMLError:
                log_config_event("yaml_parse_error", config_file=CONFIG_FILE)
    else:
        log_config_event("config_missing", config_file=CONFIG_FILE)

    return config