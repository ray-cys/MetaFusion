import yaml
import logging
from pathlib import Path

# Path to the user configuration file
CONFIG_FILE = Path(__file__).parent.parent / "config.yml"

# Default configuration at module level
DEFAULT_CONFIG = {
    "dry_run": False,
    "plex": {
        "url": "",
        "token": ""
    },
    "tmdb": {
        "api_key": "",
        "language": "en",
        "region": "US",
        "fallback_languages": ["zh", "ja", "zh-yue"]
    },
    "metadata_path": "metadata",
    "assets": {
        "assets_path": "assets",
        "poster_filename": "poster.jpg",
        "season_filename": "Season{season_number:02}.jpg",
        "cleanup_orphans": True,
        "thread_count": 10
    },
    "poster_selection": {
        "preferred_width": 2000,
        "preferred_height": 3000,
        "min_width": 1000,
        "min_height": 1500,
        "preferred_vote": 7.0,
        "vote_relaxed": 5.0,
        "vote_average_threshold": 5.0
    },
    "cache_file": "tmdb_cache.json",
    "preferred_libraries": ["Movies", "TV Shows"],
    "threads": {"max_workers": 5, "timeout": 300},
    "network": {
        "backoff_factor": 1,
        "max_retries": 3,
        "timeout": 10,
        "pool_connections": 100,
        "pool_maxsize": 100
    },
    "log_level": "INFO",
    "log_file": "metadata.log",
    "log_dir": "logs",
    "cleanup": {"run_by_default": True, "skip_by_default": False},
    "dry_run_default": False,
    "process_libraries": True,
    "cleanup_orphans": True 
}

def warn_unknown_keys(user_cfg, default_cfg, parent_key=""):
    """
    Warn about unknown keys in the user configuration.

    Recursively checks the user configuration dictionary for any keys
    that are not present in the default configuration, and logs a warning
    for each unknown key found.

    Args:
        user_cfg (dict): The user-provided configuration dictionary.
        default_cfg (dict): The default configuration dictionary.
        parent_key (str, optional): The parent key path for nested keys (used internally).
    """
    for key in user_cfg:
        if key not in default_cfg:
            # Log a warning for unknown keys
            full_key = f"{parent_key}.{key}" if parent_key else key
            logging.warning(f"[Config] Unknown config key in config.yml: {full_key}")
        # Recursively check nested dictionaries
        elif isinstance(user_cfg[key], dict) and isinstance(default_cfg[key], dict):
            warn_unknown_keys(user_cfg[key], default_cfg[key], parent_key=f"{parent_key}.{key}" if parent_key else key)

def deep_merge_dicts(default, user):
    """
    Recursively merge user configuration into the default configuration.

    For each key in the user configuration:
      - If the value is a dictionary and the default also has a dictionary for that key,
        merge them recursively.
      - Otherwise, override the default value with the user value.

    Args:
        default (dict): The default configuration dictionary (modified in place).
        user (dict): The user-provided configuration dictionary.
    """
    for k, v in user.items():
        if isinstance(v, dict) and isinstance(default.get(k), dict):
            # Recursively merge nested dictionaries
            deep_merge_dicts(default[k], v)
        else:
            # Override default value with user value
            default[k] = v

def load_config():
    """
    Load the configuration from config.yml, merging it with the default configuration.

    Loads the user configuration from config.yml (if it exists), warns about any unknown keys,
    and merges it recursively into the default configuration. If the config file is missing
    or invalid, the default configuration is used.

    Returns:
        dict: The merged configuration dictionary.
    """
    # Start with a copy of the default config
    config = DEFAULT_CONFIG.copy()
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            try:
                user_config = yaml.safe_load(f) or {}
                # Warn about unknown keys in user config
                warn_unknown_keys(user_config, DEFAULT_CONFIG)
                # Merge user config into default config
                deep_merge_dicts(config, user_config)
            except yaml.YAMLError:
                logging.error("[Config] Failed to parse config.yml. Using default configuration.")
    return config