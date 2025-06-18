import logging
import sys
import os
import platform
import psutil
from pathlib import Path

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

def log_hardware_info(logger):
    os_info = f"{platform.system()} {platform.release()}"
    py_version = platform.python_version()
    cpu_cores = os.cpu_count()
    ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    logger.info(f"[System] Operating System: {os_info}")
    logger.info(f"[System] Python version: {py_version}")
    logger.info(f"[System] CPU cores: {cpu_cores}")
    logger.info(f"[System] RAM: {ram_gb:.2f} GB")

def check_requirements(logger):
    py_version = sys.version_info
    cpu_cores = os.cpu_count()
    ram_gb = psutil.virtual_memory().total / (1024 ** 3)

    if py_version < MIN_PYTHON:
        logger.error(f"[System Error] Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required. Detected: {platform.python_version()}")
        sys.exit(1)
    if cpu_cores is not None and cpu_cores < MIN_CPU_CORES:
        logger.error(f"[System Error] At least {MIN_CPU_CORES} CPU cores required. Detected: {cpu_cores}")
        sys.exit(1)
    if ram_gb < MIN_RAM_GB:
        logger.error(f"[System Error] At least {MIN_RAM_GB} GB RAM required. Detected: {ram_gb:.2f} GB")
        sys.exit(1)
    log_hardware_info(logger)

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
    minutes, seconds = divmod(int(elapsed_time), 60)
    meta_summary_banner(logger)
    logger.info(f"[Summary] Processing completed in {minutes} mins {seconds} secs.")

    skipped_libraries = [lib.title for lib in libraries if lib.title not in selected_libraries]
    logger.info(
        f"[Summary] Libraries processed: {len(library_item_counts)} | "
        f"skipped: {', '.join(skipped_libraries) if skipped_libraries else 'None'}"
    )
    processed_count = sum(library_item_counts.values())
    logger.info(f"[Summary] Total items processed: {processed_count}")

    logger.info("[ Per-Library Items Count ]")
    for lib_name, count in library_item_counts.items():
        logger.info(f"  - {lib_name}: {count} items processed")

    logger.info("[ Per-Library Metadata Statistics ]")
    for lib_name, summary in metadata_summaries.items():
        logger.info(
            f"  - {lib_name}: {summary['complete']}/{summary['total_items']}, {summary['percent_complete']}% complete, "
            f"{summary['incomplete']} incomplete"
        )

    logger.info("[ Per-Library Downloaded Asset Size ]")
    for lib_name, size in library_filesize.items():
        logger.info(f"  - {lib_name}: Total {human_readable_size(size)}")
    total_asset_size = sum(library_filesize.values())
    logger.info(f"  - Total assets downloaded: {human_readable_size(total_asset_size)}")

    logger.info("[ Total Cleanup Statistics ]")
    if cleanup_orphans:
        logger.info(f"  - Titles removed from MetaFusion: {orphans_removed}")
    logger.info("=" * 50)

    if config["settings"].get("dry_run", False):
        logger.info("[Dry Run] Completed. No files were written.")
            
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