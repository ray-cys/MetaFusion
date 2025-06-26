import os
from ruamel.yaml import YAML
from pathlib import Path
from helper.logging import log_config_event

CONFIG_FILE = Path(
    os.environ.get(
        "CONFIG_FILE",
        str(Path(__file__).parent.parent / "config.yml")
    )
)

DEFAULT_CONFIG = {
    "metafusion_run": True,
    "settings": {
        "dry_run": False,
        "log_level": "INFO",
    },
    "plex": {
        "url": "PLEX_URL",
        "token": "PLEX_TOKEN",
    },
    "plex_libraries": [
        "Movies", "TV Shows"
    ],
    "tmdb": {
        "api_key": "TMDB_API_KEY",
        "language": "en",
        "region": "US",
        "fallback": ["zh", "ja"]
    },
    "metadata": {
        "directory": "metadata",
        "run_basic": True,
        "run_enhanced": True,
    },
    "assets": {
        "path": "assets",
        "mode": "kometa",
        "run_poster": True,
        "run_season": True,
        "run_background": False,
    },
    "cleanup": {
        "run_process": False
    },
    "poster_set": {
        "max_width": 2000,
        "max_height": 3000,
        "min_width": 1000,
        "min_height": 1500,
        "prefer_vote": 5.0,
        "vote_relaxed": 3.5,
        "vote_threshold": 5.0
    },
    "background_set": {
        "max_width": 3840,
        "max_height": 2160,
        "min_width": 1920,
        "min_height": 1080,
        "prefer_vote": 5.0,
        "vote_relaxed": 3.5,
        "vote_threshold": 5.0
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

def env_variable_overrides(config, prefix=""):
    for key, value in config.items():
        env_key = (prefix + "_" + key).upper() if prefix else key.upper()
        if isinstance(value, dict):
            env_variable_overrides(value, env_key)
        else:
            env_val = os.environ.get(env_key)
            if env_val is not None:
                old_val = config[key]
                if isinstance(value, bool):
                    config[key] = env_val.lower() in ("1", "true", "yes", "on")
                elif isinstance(value, int):
                    try:
                        config[key] = int(env_val)
                    except ValueError:
                        log_config_event(
                            "env_override_invalid", env_key=env_key, env_val=env_val,
                            expected_type="int", old_val=old_val
                        )
                        config[key] = value
                elif isinstance(value, float):
                    try:
                        config[key] = float(env_val)
                    except ValueError:
                        log_config_event(
                            "env_override_invalid", env_key=env_key, env_val=env_val,
                            expected_type="float", old_val=old_val
                        )
                        config[key] = value
                else:
                    config[key] = env_val
                log_config_event("env_override", env_key=env_key, env_val=env_val, old_val=old_val)

def load_config_file():
    if not CONFIG_FILE.exists():
        template_path = Path(__file__).parent.parent / "config_template.yml"
        if template_path.exists():
            import shutil
            shutil.copy(template_path, CONFIG_FILE)
            log_config_event("yaml_not_found", config_file=CONFIG_FILE)
        else:
            log_config_event("yaml_missing", config_file=CONFIG_FILE)

    config = DEFAULT_CONFIG.copy()
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            try:
                yaml = YAML()
                user_config = yaml.load(f) or {}
                warn_unknown_keys(user_config, DEFAULT_CONFIG)
                merge_config_dicts(config, user_config)
                log_config_event("config_loaded", config_file=CONFIG_FILE)
            except yaml.YAMLError:
                log_config_event("yaml_parse_error", config_file=CONFIG_FILE)
    else:
        log_config_event("config_missing", config_file=CONFIG_FILE)

    env_variable_overrides(config)
    return config