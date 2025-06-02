import logging
import sys
from pathlib import Path

def setup_logging(config):
    """
    Set up logging for the application.
    """
    # Determine the script directory and log directory
    script_name = Path(sys.argv[0]).stem
    log_dir = Path(__file__).parent.parent 
    log_dir = log_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{script_name}.log"
    prev_log = log_dir / f"previous-{script_name}.log"
    
    # Rotate previous log file if it exists
    if log_file.exists():
        if prev_log.exists():
            prev_log.unlink()
        log_file.rename(prev_log)

    # Set log level from config, default to INFO
    log_level_str = config.get("log_level", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Clear existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set log format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler for logging to file
    file_handler = logging.FileHandler(log_file, mode='w', encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger