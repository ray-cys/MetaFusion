import logging
import sys
import os
import platform
import textwrap
import requests
import psutil
from pathlib import Path

MIN_PYTHON = (3, 8)
MIN_CPU_CORES = 4
MIN_RAM_GB = 4

def get_setup_logging(config):
    script_name = Path(sys.argv[0]).stem
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{script_name}.log"

    for i in range(5, 0, -1):
        src = log_dir / f"{script_name}{'' if i == 1 else i-1}.log"
        dst = log_dir / f"{script_name}{i}.log"
        if src.exists():
            if i == 5:
                src.unlink()
            else:
                src.rename(dst)
    if log_file.exists():
        log_file.rename(log_dir / f"{script_name}1.log")

    log_level_str = config["settings"].get("log_level", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_file, mode='w', encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def get_meta_banner(logger=None, width=60):
    border = "=" * width
    title = " ".join("METAFUSION").center(width - 6)
    centered = f"|| {title} ||"
    lines = [
        border,
        centered,
        border,
    ]
    if logger:
        for line in lines:
            logger.info(line)
    else:
        for line in lines:
            print(line)

def log_hardware_info(logger):
    os_info = f"{platform.system()} {platform.release()}"
    py_version = platform.python_version()
    cpu_cores = os.cpu_count()
    ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    logger.info(f"[System] Operating System: {os_info}")
    logger.info(f"[System] Python version: {py_version}")
    logger.info(f"[System] CPU cores: {cpu_cores}")
    logger.info(f"[System] RAM: {ram_gb:.2f} GB")

def check_sys_requirements(logger, config):
    py_version = sys.version_info
    cpu_cores = os.cpu_count()
    ram_gb = psutil.virtual_memory().total / (1024 ** 3)

    if py_version < MIN_PYTHON:
        logger.error(f"[System] Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required. Detected: {platform.python_version()}. Exiting.")
        sys.exit(1)
    if cpu_cores is not None and cpu_cores < MIN_CPU_CORES:
        logger.error(f"[System] At least {MIN_CPU_CORES} CPU cores required. Detected: {cpu_cores}. Exiting.")
        sys.exit(1)
    if ram_gb < MIN_RAM_GB:
        logger.error(f"[System] At least {MIN_RAM_GB} GB RAM required. Detected: {ram_gb:.2f} GB. Exiting.")
        sys.exit(1)
    log_hardware_info(logger)

    plex_url = config.get('plex', {}).get('url')
    plex_token = config.get('plex', {}).get('token')
    internal_up = False
    if plex_url and plex_token:
        try:
            url = f"{plex_url}/?X-Plex-Token={plex_token}"
            resp = requests.get(url, timeout=2)
            internal_up = resp.status_code in (200, 401)
        except Exception as e:
            logger.error(f"[Network] Internal network (Plex server) check failed: {e}")
    else:
        logger.error("[Network] Plex server URL or token not set in config.")

    tmdb_api_key = config.get('tmdb', {}).get('api_key')
    tmdb_up = False
    if tmdb_api_key:
        tmdb_url = f"https://api.themoviedb.org/3/configuration?api_key={tmdb_api_key}"
        try:
            resp = requests.get(tmdb_url, timeout=3)
            tmdb_up = resp.status_code == 200
        except Exception as e:
            logger.error(f"[Network] TMDb API check failed: {e}")
    else:
        logger.error("[Network] TMDb API key not set in config.")

    if internal_up and tmdb_up:
        logger.info("[Network] Plex server and TMDb API are UP.")
    else:
        if not internal_up:
            logger.error("[Network] Plex server is DOWN. Exiting.")
        if not tmdb_up:
            logger.error("[Network] TMDb API is DOWN. Exiting.")
        sys.exit(1)

def log_helper_event(event, **kwargs):
    logger = kwargs.get("logger") or logging.getLogger()
    messages = {
        # Main events
        "metafusion_processing_disabled": "[MetaFusion] Processing is set to False. Exiting without changes.",
        "metafusion_started": "[MetaFusion] Started at {start_time}",
        "metafusion_processing_metadata": "[MetaFusion] Processing Plex libraries metadata...",
        "metafusion_no_libraries": "[MetaFusion] No libraries scheduled to process.",
        "metafusion_unhandled_exception": "[MetaFusion] Unhandled exception: {error}",
        # Config events
        "feature_disabled": "[Config] {feature} is DISABLED and will not run.",
        "feature_enabled": "[Config] {feature} is ENABLED and will run.",
        "unknown_key": "[Config] Unknown config key in config.yml: {key}",
        "yaml_not_found": "[Config] YAML not found at {config_file}. Copying template to {config_file}. Review and edit configuration.",
        "yaml_missing": "[Config] YAMLs not found at {config_file}. Using default configuration.",
        "yaml_parse_error": "[Config] Failed to parse YAML at {config_file}. Using default configuration.",
        "config_missing": "[Config] Config file {config_file} does not exist. Using default configuration.",
        "env_override": "[Config] Environment override: {env_key}={env_val} (was {old_val})",
        "config_loaded": "[Config] Successfully loaded configuration from {config_file}.",
        # Plex events
        "Plex_connected": "[Plex] Successfully connected to Plex Server.",
        "Plex_connect_failed": "[Plex] Failed to connect to Plex Server: {error}",
        "Plex_libraries_retrieved_failed": "[Plex] Failed to retrieve libraries: {error}",
        "Plex_detected_libraries": "[Plex] Detected libraries [  {libraries}  ]",
        "Plex_no_libraries_found": "[Plex] No libraries found. Exiting.",
        "Plex_skipping_library": "[Plex] Skipping library [  {library}  ]",
        "Plex_failed_extract_item_id": "[Plex] Failed to extract item ID for {title} ({year}): {error}",
        "Plex_failed_extract_library_type": "[Plex] Failed to extract library type for {library_name}: {error}",
        "Plex_failed_extract_ids": "[Plex] Failed to extract TMDb, IMDb, TVDb IDs for {title} ({year}): {error}",
        "Plex_missing_ids": "[Plex] Missing IDs for {title} ({year}): {missing_ids}. Extracted: {found_ids}",
        "Plex_failed_extract_movie_dir": "[Plex] Failed to extract movie directory for {title} ({year}): {error}",
        "Plex_failed_extract_show_dir": "[Plex] Failed to extract show directory for {title} ({year}): {error}",
        "Plex_failed_extract_seasons_episodes": "[Plex] Failed to extract seasons/episodes for {title} ({year}): {error}",
        "Plex_critical_metadata_missing": "[Plex] Critical metadata missing for item [ratingKey={item_key}]: {missing_critical}. Extracted: {result}",
        # TMDb events
        "TMDb_no_session": "[TMDb] No aiohttp session provided for TMDb API request.",
        "TMDb_no_api_key": "[TMDb] No API key found in config: {tmdb_config}",
        "TMDb_cache_hit": "[TMDb] Returning cached response for {url} params: {params}",
        "TMDb_request": "[TMDb] Making request to {url} with params: {query} (Attempt {attempt}/{retries})",
        "TMDb_success": "[TMDb] Successful response for {url} (Attempt {attempt})",
        "TMDb_rate_limited": "[TMDb] Rate limited (HTTP 429). Sleeping {retry_after}s before retry... Params: {query}",
        "TMDb_non_200": "[TMDb] Non-200 response {status} for {url} params: {query} body: {body}",
        "TMDb_request_failed": "[TMDb] Attempt {attempt}: Request failed for URL {url} with params {query}: {error}",
        "TMDb_retrying": "[TMDb] Retrying in {sleep_time}s... (Attempt {next_attempt}/{retries})",
        "TMDb_failed": "[TMDb] Failed after {retries} attempts for {url} with params {query}",
    }
    levels = {
        # Main events
        "metafusion_processing_disabled": "info",
        "metafusion_started": "info",
        "metafusion_processing_metadata": "info",
        "metafusion_no_libraries": "info",
        "metafusion_unhandled_exception": "error",
        # Config events
        "feature_disabled": "info",
        "feature_enabled": "info",
        "unknown_key": "warning",
        "yaml_not_found": "warning",
        "yaml_missing": "error",
        "yaml_parse_error": "error",
        "config_missing": "warning",
        "env_override": "debug",
        "config_loaded": "debug",
        # Plex events
        "Plex_connected": "debug",
        "Plex_connect_failed": "error",
        "Plex_libraries_retrieved_failed": "error",
        "Plex_detected_libraries": "info",
        "Plex_no_libraries_found": "warning",
        "Plex_skipping_library": "info",
        "Plex_failed_extract_item_id": "warning",
        "Plex_failed_extract_library_type": "warning",
        "Plex_failed_extract_ids": "warning",
        "Plex_missing_ids": "debug",
        "Plex_failed_extract_movie_dir": "warning",
        "Plex_failed_extract_show_dir": "warning",
        "Plex_failed_extract_seasons_episodes": "warning",
        "Plex_critical_metadata_missing": "warning",
        # TMDb events
        "TMDb_no_session": "error",
        "TMDb_no_api_key": "error",
        "TMDb_cache_hit": "debug",
        "TMDb_request": "debug",
        "TMDb_success": "debug",
        "TMDb_rate_limited": "warning",
        "TMDb_non_200": "warning",
        "TMDb_request_failed": "warning",
        "TMDb_retrying": "info",
        "TMDb_failed": "error",
    }
    msg = messages.get(event, "[Logging] Unknown event")
    try:
        msg = msg.format(**kwargs)
    except Exception:
        pass
    level = levels.get(event, "info")
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.debug(msg)

def log_processing_event(event, **kwargs):
    logger = kwargs.get("logger") or logging.getLogger()
    messages = {
        "processing_no_item": "[Processing] No item found. Skipping...",
        "processing_dry_run": "[Dry Run] Would process metadata and assets for: {full_title} ({library_name})",
        "processing_started": "[Processing] Started processing: {full_title}",
        "processing_unsupported_type": "[Processing] Unsupported library type for {full_title}. Skipping...",
        "processing_failed": "[Processing] Failed to process {full_title}: {error}",
        "processing_finished": "[Processing] Finished processing: {full_title}",
        "processing_library_items": "[Processing] {library_name} with {total_items} items.",
        "processing_failed_parse_yaml": "[Processing] Failed to parse YAML file: {output_path} ({error})",
        "processing_saving_metadata": "[Processing] Saving metadata to {output_path}...",
        "processing_metadata_saved": "[Processing] Metadata successfully saved to {output_path}",
        "processing_saving_cache": "[Processing] Saving cache files...",
        "processing_cache_saved": "[Processing] Cache files saved.",
        "processing_failed_write_metadata": "[Processing] Failed to write metadata: {error}",
        "processing_metadata_dry_run": "[Dry Run] Metadata for {library_name} generated but not saved.",
        "processing_failed_library": "[Processing] Failed to process library '{library_name}': {error}",
        "processing_summary_line": "{line}",
    }
    levels = {
        "processing_no_item": "warning",
        "processing_dry_run": "info",
        "processing_started": "debug",
        "processing_unsupported_type": "warning",
        "processing_failed": "error",
        "processing_finished": "debug",
        "processing_library_items": "info",
        "processing_failed_parse_yaml": "error",
        "processing_saving_metadata": "debug",
        "processing_metadata_saved": "debug",
        "processing_saving_cache": "debug",
        "processing_cache_saved": "debug",
        "processing_failed_write_metadata": "error",
        "processing_metadata_dry_run": "info",
        "processing_failed_library": "error",
        "processing_summary_line": "info",
    }
    msg = messages.get(event, "[Processing] Unknown event")
    try:
        msg = msg.format(**kwargs)
    except Exception:
        pass
    level = levels.get(event, "info")
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.debug(msg)

def log_builder_event(event, **kwargs):
    logger = kwargs.get("logger") or logging.getLogger()
    messages = {
        # General and metadata events
        "builder_no_tmdb_id": "[{media_type}] No TMDb or {id_type} ID found for {full_title}. Skipping...",
        "builder_invalid_tmdb_id": "[{media_type}] Invalid TMDb ID format for {full_title}. Skipping...",
        "builder_no_tmdb_data": "[{media_type}] No TMDb data found for {full_title}. Skipping...",
        "builder_no_tmdb_season_data": "[{media_type}] No TMDb data found for Season {season_number} of {full_title}. Skipping...",
        "builder_no_metadata_changes": "[{media_type}] No metadata changes needed for {full_title}, ({percent}%) completed. Skipping updates...",
        "builder_no_existing_metadata": "[{media_type}] No existing metadata for {full_title}. Creating new entries using TMDb ID {tmdb_id}.",
        "builder_metadata_upgraded": "[{media_type}] Metadata upgraded and cache updated for {full_title} ({percent}%) using TMDb ID {tmdb_id}. Fields changed: {changes}",
        "builder_dry_run_metadata": "[Dry Run] Would build metadata for {media_type}: {full_title}",
        # Poster/Background/Collection events
        "builder_no_asset_path": "[{media_type}] Asset path could not be determined for {full_title}{extra}. Skipping {asset_type} download...",
        "builder_no_suitable_asset": "[{media_type}] No suitable {asset_type} found for {full_title}{extra}.",
        "builder_downloading_asset": "[{media_type}] Downloading {asset_type} for {full_title} from TMDb path: {file_path}. Filesize: {filesize}",
        "builder_asset_download_failed": "[{media_type}] New {asset_type} download failed for {full_title} from {url} (status: {status}) Error: {error}",
        "builder_asset_upgraded": "[{media_type}] {asset_type} upgrade for {full_title}: {reason} Filesize: {filesize}",
        "builder_no_upgrade_needed": "[{media_type}] No {asset_type} upgrade needed for {full_title}. Existing {filesize} image meets criteria.",
        "builder_no_image_for_compare": "[{media_type}] No image provided for comparison for {full_title}{extra}. Skipping detailed check...",
        "builder_error_image_compare": "[{media_type}] Failed to read temp image for comparison for {full_title}{extra}: {error}",
        # Collection events
        "builder_no_tmdb_collection_data": "[{media_type}] No TMDb data found for collection {collection_name}. Skipping collection assets...",
        # Season events
        "builder_no_asset_path_season": "[{media_type}] No asset path found for {full_title} Season {season_number}. Skipping season poster asset...",
        "builder_no_season_details": "[{media_type}] No season details for {full_title} Season {season_number}. Skipping season poster asset...",
        "builder_no_suitable_asset_season": "[{media_type}] No suitable {asset_type} found for {full_title} Season {season_number}. Skipping...",
        "builder_downloading_asset_season": "[{media_type}] Downloading {asset_type} for {full_title} Season {season_number} from TMDb path: {file_path}. Filesize: {filesize}",
        "builder_asset_download_failed_season": "[{media_type}] {asset_type} download failed for {full_title} Season {season_number} from {url} (status: {status}) Error: {error}",
        "builder_asset_upgraded_season": "[{media_type}] Season {season_number} {asset_type} upgraded for {full_title}: {reason} Filesize: {filesize}",
        "builder_no_upgrade_needed_season": "[{media_type}] Season {season_number}: No {asset_type} upgrade needed. Existing {filesize} image meets criteria.",
        "builder_no_image_for_compare_season": "[{media_type}] Season {season_number}: No image provided for comparison. Skipping detailed check...",
        "builder_error_image_compare_season": "[{media_type}] Season {season_number}: Failed to read temp image for comparison: {error}",
    }
    levels = {
        # General and metadata events
        "builder_no_tmdb_id": "warning",
        "builder_invalid_tmdb_id": "warning",
        "builder_no_tmdb_data": "warning",
        "builder_no_tmdb_season_data": "warning",
        "builder_no_metadata_changes": "info",
        "builder_no_existing_metadata": "info",
        "builder_metadata_upgraded": "info",
        "builder_dry_run_metadata": "info",
        # Poster/Background/Collection events
        "builder_no_asset_path": "error",
        "builder_no_suitable_asset": "info",
        "builder_downloading_asset": "debug",
        "builder_asset_download_failed": "error",
        "builder_asset_upgraded": "info",
        "builder_no_upgrade_needed": "info",
        "builder_no_image_for_compare": "warning",
        "builder_error_image_compare": "error",
        # Collection events
        "builder_no_tmdb_collection_data": "warning",
        # Season events
        "builder_no_asset_path_season": "warning",
        "builder_no_season_details": "info",
        "builder_no_suitable_asset_season": "info",
        "builder_asset_download_failed_season": "error",
        "builder_asset_upgraded_season": "info",
        "builder_no_upgrade_needed_season": "info",
        "builder_no_image_for_compare_season": "warning",
        "builder_error_image_compare_season": "error",
    }
    # Handle dynamic reason construction for asset upgrades
    if event == "builder_asset_upgraded":
        status_code = kwargs.get("status_code")
        context = kwargs.get("context", {})
        asset_type = kwargs.get("asset_type", "")
        if status_code == "UPGRADE_VOTES":
            reason = f"Higher vote found: {context.get('new_votes')} (Cached: {context.get('cached_votes')}, Threshold: {context.get('vote_threshold')})"
        elif status_code == "UPGRADE_THRESHOLD":
            reason = f"Meeting vote threshold: {context.get('new_votes')} (Threshold: {context.get('vote_threshold')})"
        elif status_code == "UPGRADE_DIMENSIONS":
            reason = f"New dimensions: {context.get('new_width')}x{context.get('new_height')}, Existing: {context.get('existing_width', '?')}x{context.get('existing_height', '?')}"
        else:
            reason = ""
        kwargs["reason"] = reason
    if event == "builder_asset_upgraded_season":
        status_code = kwargs.get("status_code")
        context = kwargs.get("context", {})
        asset_type = kwargs.get("asset_type", "")
        if status_code == "UPGRADE_VOTES":
            reason = f"Higher vote found: {context.get('new_votes')} (Cached: {context.get('cached_votes')}, Threshold: {context.get('vote_threshold')})"
        elif status_code == "UPGRADE_THRESHOLD":
            reason = f"Meeting vote threshold: {context.get('new_votes')} (Threshold: {context.get('vote_threshold')})"
        elif status_code == "UPGRADE_DIMENSIONS":
            reason = f"New dimensions: {context.get('new_width')}x{context.get('new_height')}, Existing: {context.get('existing_width', '?')}x{context.get('existing_height', '?')}"
        else:
            reason = ""
        kwargs["reason"] = reason
        
    msg = messages.get(event, "[Builder] Unknown event")
    try:
        msg = msg.format(**kwargs)
    except Exception:
        pass
    level = levels.get(event, "info")
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.debug(msg)

def log_asset_status(status_code, *, media_type, asset_type, full_title, filesize=None, error=None, extra=None, season_number=None):
    """
    Centralized handler for asset status logging.
    """
    event_map = {
        "NO_UPGRADE_NEEDED": "builder_no_upgrade_needed",
        "NO_IMAGE_FOR_COMPARE": "builder_no_image_for_compare",
        "ERROR_IMAGE_COMPARE": "builder_error_image_compare",
        "NO_UPGRADE_NEEDED_SEASON": "builder_no_upgrade_needed_season",
        "NO_IMAGE_FOR_COMPARE_SEASON": "builder_no_image_for_compare_season",
        "ERROR_IMAGE_COMPARE_SEASON": "builder_error_image_compare_season",
    }
    event = event_map.get(status_code)
    if not event:
        return
    kwargs = {
        "media_type": media_type,
        "asset_type": asset_type,
        "full_title": full_title,
    }
    if filesize is not None:
        kwargs["filesize"] = filesize
    if error is not None:
        kwargs["error"] = error
    if extra is not None:
        kwargs["extra"] = extra
    if season_number is not None:
        kwargs["season_number"] = season_number
    log_builder_event(event, **kwargs)

def log_cleanup_event(event, **kwargs):
    logger = kwargs.get("logger") or logging.getLogger()
    messages = {
        "cleanup_start": "[Cleanup] Starting titles cleanup...",
        "cleanup_removed_cache_entry": "[Cleanup] Removed TMDb cache entry: {key}",
        "cleanup_skipping_nonpreferred": "[Cleanup] Skipping non-preferred library: {filename}",
        "cleanup_removed_orphans": "[Cleanup] Removed {orphans_in_file} entries from {filename}",
        "cleanup_failed_remove_metadata": "[Cleanup] Failed to remove {filename}: {error}",
        "cleanup_skipping_collection_asset": "[Cleanup] Skipping collection asset {description}: {path}",
        "cleanup_skipping_valid_asset": "[Cleanup] Skipping valid asset {description}: {path}",
        "cleanup_removing_asset": "[Cleanup] {action_msg} cleanup {description}: {path}",
        "cleanup_removing_empty_dir": "[Cleanup] {parent_action_msg} empty directory: {parent}",
        "cleanup_failed_remove_asset": "[Cleanup] Failed to remove {description} {path}: {error}",
        "cleanup_total_removed": "[Cleanup] Total titles removed: {orphans_removed}",
    }
    levels = {
        "cleanup_start": "info",
        "cleanup_removed_cache_entry": "info",
        "cleanup_skipping_nonpreferred": "info",
        "cleanup_removed_orphans": "info",
        "cleanup_failed_remove_metadata": "error",
        "cleanup_skipping_collection_asset": "info",
        "cleanup_skipping_valid_asset": "info",
        "cleanup_removing_asset": "info",
        "cleanup_removing_empty_dir": "info",
        "cleanup_failed_remove_asset": "warning",
        "cleanup_total_removed": "info",
    }
    msg = messages.get(event, "[Cleanup] Unknown event")
    try:
        msg = msg.format(**kwargs)
    except Exception:
        pass
    level = levels.get(event, "info")
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.debug(msg)

def log_library_summary(
    library_name,
    completed,
    incomplete,
    total_items,
    percent_complete,
    asset_summaries,
    library_filesize=None,
    run_metadata=True,
    logger=None,
    box_width=60
):
    logger = logger or logging.getLogger()
    def box_line(text, width=box_width):
        import textwrap
        wrapped = textwrap.wrap(text, width=width-4)
        return [f"|| {line.ljust(width-4)}||".rstrip() for line in wrapped]

    header = "=" * box_width
    title = "LIBRARY PROCESSING SUMMARY".center(box_width - 8)
    lines = [
        header,
        f"||{title}||",
        header,
        f"|| Library: {library_name.ljust(box_width - 16)}||"
    ]
    # Dynamic metadata, asset, item summary
    if run_metadata:
        meta_line = f"Metadata: {completed}/{total_items} completed, {incomplete} incomplete ({percent_complete}%)"
        lines.extend(box_line(meta_line, box_width))
    if asset_summaries:
        asset_line = f"Assets: {', '.join(asset_summaries)}"
        lines.extend(box_line(asset_line, box_width))
    items_line = f"Items: total processed {total_items} titles"
    lines.extend(box_line(items_line, box_width))
    lines.append(header)
    for line in lines:
        logger.info(line)

def log_final_summary(
    logger,
    elapsed_time,
    library_item_counts,
    metadata_summaries,
    library_filesize,
    orphans_removed,
    cleanup_orphans,
    selected_libraries,
    libraries,
    config,
):
    box_width = 60
    def box_line(text, width=box_width):
        wrapped = textwrap.wrap(text, width=width-4)
        return [f"|| {line.ljust(width-4)}||" for line in wrapped]

    border = "=" * box_width
    title = "METAFUSION SUMMARY REPORT".center(box_width - 6)
    lines = [
        border,
        f"||{title}||",
        border
    ]
    minutes, seconds = divmod(int(elapsed_time), 60)
    lines.extend(box_line(f"Processing completed in {minutes} mins {seconds} secs.", box_width))

    skipped_libraries = [lib["title"] for lib in libraries if lib["title"] not in selected_libraries]
    lines.extend(box_line(f"Libraries processed: {len(library_item_counts)} | skipped: {', '.join(skipped_libraries) if skipped_libraries else 'None'}", box_width))

    # Items summary
    items_str = ", ".join(f"{lib} ({count})" for lib, count in library_item_counts.items())
    lines.extend(box_line(f"Items: {items_str}", box_width))

    # Metadata summary
    meta_str = ", ".join(
        f"{lib} ({summary['complete']}/{summary['total_items']}, {summary['percent_complete']}%, {summary['incomplete']} incomplete)"
        for lib, summary in metadata_summaries.items()
    )
    lines.extend(box_line(f"Metadata: {meta_str}", box_width))

    # Assets summary
    assets_str = ", ".join(f"{lib} ({human_readable_size(size)})" for lib, size in library_filesize.items())
    total_asset_size = sum(library_filesize.values())
    assets_line = f"Assets: {assets_str}, Total ({human_readable_size(total_asset_size)})"
    lines.extend(box_line(assets_line, box_width))

    # Cleanup summary
    if cleanup_orphans:
        lines.extend(box_line(f"Cleanup: Titles removed: {orphans_removed}", box_width))
    if config["settings"].get("dry_run", False):
        lines.extend(box_line("[Dry Run] Completed. No files were written.", box_width))
    lines.append(border)
    for line in lines:
        logger.info(line)

def meta_summary_banner(logger=None, width=50):
    border = "=" * width
    title = "METAFUSION SUMMARY REPORT".center(width - 4)
    centered = f"|| {title} ||"
    lines = [
        border,
        centered,
        border,
    ]
    if logger:
        for line in lines:
            logger.info(line)
    else:
        for line in lines:
            print(line)
            
def human_readable_size(size, decimal_places=2):
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0 or unit == 'TB':
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024.0