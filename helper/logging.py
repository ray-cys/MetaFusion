import logging
import sys
import os
import platform
import textwrap
import requests
import psutil
from pathlib import Path
from helper.config import load_config

MIN_PYTHON = (3, 8)
MIN_CPU_CORES = 4
MIN_RAM_GB = 4

def setup_logging(config):
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

def meta_banner(logger=None, width=50):
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

def hardware_info(logger):
    os_info = f"{platform.system()} {platform.release()}"
    py_version = platform.python_version()
    cpu_cores = os.cpu_count()
    ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    logger.info(f"[System] Operating System: {os_info}")
    logger.info(f"[System] Python version: {py_version}")
    logger.info(f"[System] CPU cores: {cpu_cores}")
    logger.info(f"[System] RAM: {ram_gb:.2f} GB")

def check_requirements(logger):
    config = load_config()
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
    hardware_info(logger)

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

def log_summary_report(
    logger,
    elapsed_time,
    library_item_counts,
    metadata_summaries,
    library_filesize,
    orphans_removed,
    cleanup_orphans,
    selected_libraries,
    libraries,
    config
):
    box_width = 60
    def box_line(text, width=box_width):
        wrapped = textwrap.wrap(text, width=width-4)
        return [f"|| {line.ljust(width-4)}||" for line in wrapped]

    border = "=" * box_width
    title = "METAFUSION SUMMARY REPORT".center(box_width - 2)
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
    logger.info("\n" + "\n".join(lines))

def meta_summary_banner(logger=None, width=50):
    border = "=" * width
    title = "METAFUSION SUMMARY REPORT".center(width - 6)
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