#!/usr/bin/env python3
# ---------------------------------------------------------
# Plex Metadata & Asset Generator
# Features:
# - Extracts and processes libraries from Plex
# - Uses multi-threading for fast, parallel metadata and asset processing
# - Fetches and consolidates TMDb metadata for movies and TV shows
# - Downloads, upgrades, and manages poster/season assets
# - Cleans up orphaned metadata and asset files for a tidy library
# - Outputs YAML metadata compatible with Kometa and similar tools
# ---------------------------------------------------------

import sys
import time
from pathlib import Path
from helper.config import load_config
from helper.logging import setup_logging
from helper.plex import get_plex_libraries
from plexapi.server import PlexServer
from modules.processing import process_library, process_library_assets
from modules.cleanup import cleanup_orphans

# Load configuration and set up logger
config = load_config()
logger = setup_logging(config)

if __name__ == "__main__":
    """
    Main entry point for the Plex Metadata Generator script.

    - Connects to Plex server.
    - Processes selected libraries for metadata and assets.
    - Optionally cleans up orphaned metadata and assets.
    - Logs a summary report at the end.
    """
    try:
        start_time = time.time()
        library_item_counts = {}

        # Get list of libraries to process from config
        selected_libraries = config.get("preferred_libraries", ["Movies", "TV Shows"])

        # Connect to Plex server
        try:
            plex = PlexServer(config["plex"]["url"], config["plex"]["token"])
            logger.info("[Startup] Successfully connected to Plex.")
        except Exception as e:
            logger.error(f"[Startup] Failed to connect to Plex: {e}")
            sys.exit(1)

        # Retrieve Plex libraries
        libraries = get_plex_libraries(plex)
        if not libraries:
            logger.warning("[Startup] No Plex libraries found. Exiting.")
            sys.exit(0)

        orphans_removed = 0

        # Flags for processing and cleanup
        process_libraries = config.get("process_libraries", True)
        cleanup_orphans_flag = config.get("cleanup_orphans", True)

        # Asset summary tracking
        asset_summary = {}

        # Process each library
        if process_libraries:
            library_percentages = {}
            for lib in libraries:
                library_name = lib.get("title")
                if library_name not in selected_libraries:
                    logger.info(f"[Library Skip] Skipping library: {library_name}")
                    continue
                logger.info(f"[Library Start] Processing library: {library_name}")
                # Process metadata for the library
                process_library(
                    plex=plex,
                    library_name=library_name,
                    dry_run=config.get("dry_run_default", False),
                    library_item_counts=library_item_counts,
                )
                # Initialize per-library asset stats
                asset_summary[library_name] = {"downloaded": 0, "updated": 0, "skipped": 0, "removed": 0}
                # Process assets (e.g., posters) for the library
                process_library_assets(
                    plex=plex,
                    summary=asset_summary[library_name]
                )

        # Optionally clean up orphaned metadata and assets
        if cleanup_orphans_flag:
            orphans_removed = cleanup_orphans(
                plex,
                libraries=[lib.get("title") for lib in libraries if lib.get("title") in selected_libraries],
                asset_path=config["assets"]["assets_path"],
                poster_filename=config["assets"].get("poster_filename", "poster.jpg"),
                season_filename=config["assets"].get("season_filename", "Season{season_number:02}.jpg"),
                summary=asset_summary
            )

        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(int(elapsed_time), 60)

        # --- Summary Report ---
        logger.info("=" * 60)
        logger.info("SUMMARY REPORT")
        logger.info("=" * 60)
        logger.info(f"[Summary] Processing completed in {minutes} minutes {seconds} seconds.")
        logger.info(f"[Summary] Libraries processed: {len(library_item_counts)}")
        skipped_libraries = [lib.get("title") for lib in libraries if lib.get("title") not in selected_libraries]
        logger.info(f"[Summary] Libraries skipped: {', '.join(skipped_libraries) if skipped_libraries else 'None'}")
        processed_count = sum(library_item_counts.values())
        logger.info(f"[Summary] Total items processed: {processed_count}")
        logger.info("[Per-Library Metadata Item Stats]")
        for lib_name, count in library_item_counts.items():
            logger.info(f"  - {lib_name}: {count} items processed")
        logger.info("[Per-Library Assets Stats]")
        for lib_name, stats in asset_summary.items():
            logger.info(f"  - {lib_name}: Downloaded: {stats['downloaded']}")
            logger.info(f"  - {lib_name}: Updated: {stats['updated']}")
            logger.info(f"  - {lib_name}: Skipped: {stats['skipped']}")
            logger.info(f"  - {lib_name}: Removed: {stats['removed']}")
        if cleanup_orphans_flag:
            logger.info(f"  - Titles Removed (Orphans): {orphans_removed}")
        logger.info("=" * 60)

        if config.get("dry_run_default", False):
            logger.info("[Dry Run] Completed. No files were written.")

    except Exception as e:
        logger.error(f"[Fatal] Unhandled exception: {e}", exc_info=True)
        sys.exit(1)