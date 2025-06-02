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
from helper.config import load_config
from helper.logging import setup_logging
from helper.plex import get_plex_libraries
from helper.stats import human_readable_size 

# Load configuration and set up logger
config = load_config()
logger = setup_logging(config)

if __name__ == "__main__":
    from plexapi.server import PlexServer
    from modules.processing import process_library
    from modules.cleanup import cleanup_orphans
    """
    Main entry point for MetaFusion script.
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

        # Metadata & Asset summary tracking
        metadata_summaries = {}
        library_filesize = {}

        # Process each library
        if process_libraries:
            for lib in libraries:
                library_name = lib.get("title")
                if library_name not in selected_libraries:
                    logger.info(f"[Library Skip] Skipping library: {library_name}")
                    continue
                # Process metadata and assets for the library
                process_library(
                    plex=plex,
                    library_name=library_name,
                    dry_run=config.get("dry_run_default", False),
                    library_item_counts=library_item_counts,
                    metadata_summaries=metadata_summaries,
                    library_filesize=library_filesize,
                )

        # Optionally clean up orphaned metadata and assets
        if cleanup_orphans_flag:
            orphans_removed = cleanup_orphans(
                plex,
                libraries=[lib.get("title") for lib in libraries if lib.get("title") in selected_libraries],
                asset_path=config["assets"]["assets_path"],
                poster_filename=config["assets"].get("poster_filename", "poster.jpg"),
                season_filename=config["assets"].get("season_filename", "Season{season_number:02}.jpg"),
            )

        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(int(elapsed_time), 60)

        # --- Summary Report ---
        logger.info("=" * 60)
        logger.info("METAFUSION SUMMARY REPORT")
        logger.info("=" * 60)
        logger.info(f"[Summary] Processing completed in {minutes} minutes {seconds} seconds.")
        logger.info(f"[Summary] Libraries processed: {len(library_item_counts)}")
        skipped_libraries = [lib.get("title") for lib in libraries if lib.get("title") not in selected_libraries]
        logger.info(f"[Summary] Libraries skipped: {', '.join(skipped_libraries) if skipped_libraries else 'None'}")
        processed_count = sum(library_item_counts.values())
        logger.info(f"[Summary] Total items processed: {processed_count}")
        logger.info("[Per-Library Metadata Counts]")
        for lib_name, count in library_item_counts.items():
            logger.info(f"  - {lib_name}: {count} items processed")
        logger.info("[Per-Library Metadata Stats]")
        metadata_summaries = globals().get("metadata_summaries", {})
        for lib_name, summary in metadata_summaries.items():
            logger.info(
                f"  - {lib_name}: {summary['complete']}/{summary['total_items']} complete, {summary['incomplete']} incomplete"
            )
        logger.info("[Per-Library Downloaded Asset Size]")
        for lib_name, size in library_filesize.items():
            logger.info(f"  - {lib_name}: {human_readable_size(size)}")

        total_downloaded = sum(library_filesize.values())
        logger.info(f"[Summary] Total assets downloaded: {human_readable_size(total_downloaded)}")
        logger.info("[Cleanup Stats]")
        if cleanup_orphans_flag:
            logger.info(f"  - Titles Removed (Orphans): {orphans_removed}")
        logger.info("=" * 60)

        if config.get("dry_run_default", False):
            logger.info("[Dry Run] Completed. No files were written.")

    except Exception as e:
        logger.error(f"[Fatal] Unhandled exception: {e}", exc_info=True)
        sys.exit(1)