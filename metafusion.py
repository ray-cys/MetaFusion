import sys
import asyncio
import aiohttp
from datetime import datetime
from plexapi.server import PlexServer
from helper.config import load_config, log_disabled_features
from helper.logging import setup_logging, meta_banner, check_requirements, log_summary_report
from modules.processing import process_library
from modules.cleanup import cleanup_orphans

config = load_config()
logger = setup_logging(config)

if __name__ == "__main__":
    async def main():
        meta_banner(logger)
        logger.info(f"[MetaFusion] Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        check_requirements(logger)
        log_disabled_features(config, logger)
        start_time = datetime.now()
        library_item_counts = {}
        selected_libraries = config.get("preferred_libraries", ["Movies", "TV Shows"])

        async with aiohttp.ClientSession() as session:
            try:
                try:
                    plex = PlexServer(config["plex"]["url"], config["plex"]["token"])
                    logger.debug("[MetaFusion] Successfully connected to Plex.")
                except Exception as e:
                    logger.error(f"[MetaFusion] Failed to connect to Plex: {e}")
                    sys.exit(1)
                
                try:
                    sections = list(plex.library.sections())
                except Exception as e:
                    logger.error(f"[MetaFusion] Failed to retrieve Plex libraries: {e}", exc_info=True)
                    sys.exit(1)

                libraries = [{"title": section.title, "type": section.TYPE} for section in sections]
                logger.info(f"[MetaFusion] Detected Plex libraries: {[lib['title'] for lib in libraries]}")

                if not sections:
                    logger.warning("[MetaFusion] No Plex libraries found. Exiting.")
                    sys.exit(0)

                cleanup_orphans_flag = config.get("cleanup_processing", True)
                metadata_summaries = {}
                library_filesize = {}

                # Processing libraries
                tasks = []
                for section in libraries:
                    library_name = section.title
                    if library_name not in selected_libraries:
                        logger.info(f"[MetaFusion] Skipping Plex library: {library_name}")
                        continue

                    movie_cache = {}
                    season_cache = {}
                    episode_cache = {}

                    tasks.append(
                        process_library(
                            library_section=section,
                            dry_run=config.get("dry_run", False),
                            library_item_counts=library_item_counts,
                            metadata_summaries=metadata_summaries,
                            library_filesize=library_filesize,
                            season_cache=season_cache,
                            episode_cache=episode_cache,
                            movie_cache=movie_cache,
                            session=session
                        )
                    )

                if tasks:
                    await asyncio.gather(*tasks)
                else:
                    logger.info("[MetaFusion] No libraries scheduled to process.")

                # Clean up metadata and assets
                orphans_removed = 0
                if cleanup_orphans_flag:
                    orphans_removed = await cleanup_orphans(
                        plex,
                        libraries=[section.title for section in libraries if section.title in selected_libraries],
                        asset_path=config["assets_path"],
                    )

                # Final summary report
                end_time = datetime.now()
                elapsed_time = (end_time - start_time).total_seconds()
                log_summary_report(
                    logger,
                    elapsed_time,
                    library_item_counts,
                    metadata_summaries,
                    library_filesize,
                    orphans_removed,
                    cleanup_orphans_flag,
                    selected_libraries,
                    libraries,
                    config
                )
            except Exception as e:
                logger.error(f"[Fatal Error] Unhandled exception in main: {e}", exc_info=True)
                sys.exit(1)

    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"[Fatal Error] Unhandled exception: {e}", exc_info=True)
        sys.exit(1)