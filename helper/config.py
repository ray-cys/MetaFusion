import os
import logging
from ruamel.yaml import YAML
from pathlib import Path

# Path to the user configuration file
CONFIG_FILE = Path(
    os.environ.get(
        "CONFIG_FILE",
        str(Path(__file__).parent.parent / "config.yml")
    )
)

# Default configuration at module level
DEFAULT_CONFIG = {
    "dry_run": False,
    "log_level": "INFO",
    "plex": {
        "url": "",
        "token": ""
    },
    "tmdb": {
        "api_key": "",
        "language": "en",
        "region": "US",
        "fallback": ["zh", "ja"]
    },
    "preferred_libraries": [
        "Movies", "TV Shows"
    ],
    "process_metadata": True,
    "process_posters": True,
    "process_season_posters": True,
    "process_backgrounds": True,
    "cleanup": {"run_by_default": True, "skip_by_default": False},
    "cleanup_orphans": True,
    "metadata_path": "metadata",
    "assets_path": "assets",
    "poster_selection": {
        "preferred_width": 2000,
        "preferred_height": 3000,
        "min_width": 1000,
        "min_height": 1500,
        "preferred_vote": 7.0,
        "vote_relaxed": 5.0,
        "vote_average_threshold": 5.0
    },
    "background_selection": {
        "preferred_width": 3840,
        "preferred_height": 2160,
        "min_width": 1920,
        "min_height": 1080,
        "preferred_vote": 7.0,
        "vote_relaxed": 5.0,
        "vote_average_threshold": 5.0
    },
}

def warn_unknown_keys(user_cfg, default_cfg, parent_key=""):
    """
    Warn about unknown keys in the user configuration.
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
    """
    for k, v in user.items():
        if isinstance(v, dict) and isinstance(default.get(k), dict):
            # Recursively merge nested dictionaries
            deep_merge_dicts(default[k], v)
        else:
            # Override default value with user value
            default[k] = v

def apply_env_overrides(config, prefix=""):
    """
    Recursively override config values with environment variables.
    For nested keys, use underscores, e.g., PLEX_URL, ASSETS_PATH.
    """
    for key, value in config.items():
        env_key = (prefix + "_" + key).upper() if prefix else key.upper()
        if isinstance(value, dict):
            apply_env_overrides(value, env_key)
        else:
            env_val = os.environ.get(env_key)
            if env_val is not None:
                # Try to cast to correct type (bool, int, float)
                if isinstance(value, bool):
                    config[key] = env_val.lower() in ("1", "true", "yes", "on")
                elif isinstance(value, int):
                    try:
                        config[key] = int(env_val)
                    except ValueError:
                        config[key] = value
                elif isinstance(value, float):
                    try:
                        config[key] = float(env_val)
                    except ValueError:
                        config[key] = value
                else:
                    config[key] = env_val

def load_config():
    """
    Load the configuration from config.yml, merging it with the default configuration,
    and override any value with environment variables if present.
    """
    config = DEFAULT_CONFIG.copy()
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            try:
                yaml = YAML()
                user_config = yaml.load(f) or {}
                warn_unknown_keys(user_config, DEFAULT_CONFIG)
                deep_merge_dicts(config, user_config)
            except yaml.YAMLError:
                logging.error("[Config] Failed to parse config.yml. Using default configuration.")
    apply_env_overrides(config)
    return config