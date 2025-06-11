import sys
import time
import asyncio
import aiohttp
from helper.config import load_config, log_disabled_features
from helper.logging import setup_logging, meta_banner, meta_summary_banner
from helper.plex import get_plex_libraries
from helper.stats import human_readable_size
from helper.schedule import should_run_upgrade, set_last_run_time

config = load_config()
logger = setup_logging(config)
run_upgrade = should_run_upgrade(config)

if __name__ == "__main__":
    from plexapi.server import PlexServer
    from modules.processing import process_library_async
    from modules.cleanup import cleanup_orphans

    async def main():
        meta_banner(logger)
        log_disabled_features(config, logger)
        start_time = time.time()
        library_item_counts = {}
        selected_libraries = config.get("preferred_libraries", ["Movies", "TV Shows"])

        async with aiohttp.ClientSession() as session:
            try:
                try:
                    plex = PlexServer(config["plex"]["url"], config["plex"]["token"])
                    logger.info("[Startup] Successfully connected to Plex.")
                except Exception as e:
                    logger.error(f"[Startup] Failed to connect to Plex: {e}")
                    sys.exit(1)

                libraries = get_plex_libraries(plex)
                if not libraries:
                    logger.warning("[Startup] No Plex libraries found. Exiting.")
                    sys.exit(0)

                cleanup_orphans_flag = config.get("cleanup_orphans", True)
                metadata_summaries = {}
                library_filesize = {}

                # Upgrade scheduling
                run_upgrade = should_run_upgrade(config, cache_key="last_metadata_upgrade")

                # Prepare to process libraries
                tasks = []
                for lib in libraries:
                    library_name = lib.get("title")
                    if library_name not in selected_libraries:
                        logger.info(f"[Library Skip] Skipping library: {library_name}")
                        continue

                    movie_cache = {}
                    season_cache = {}
                    episode_cache = {}

                    tasks.append(
                        process_library_async(
                            plex=plex,
                            library_name=library_name,
                            dry_run=config.get("dry_run", False),
                            library_item_counts=library_item_counts,
                            metadata_summaries=metadata_summaries,
                            library_filesize=library_filesize,
                            season_cache=season_cache,
                            episode_cache=episode_cache,
                            movie_cache=movie_cache,
                            run_upgrade=run_upgrade,
                            session=session
                        )
                    )

                if tasks:
                    await asyncio.gather(*tasks)
                    if run_upgrade:
                        set_last_run_time(cache_key="last_metadata_upgrade")
                else:
                    logger.info("[Metadata] No libraries scheduled to process.")

                # Clean up orphaned metadata and assets (configurable in config)
                orphans_removed = 0
                if cleanup_orphans_flag:
                    orphans_removed = await cleanup_orphans(
                        plex,
                        libraries=[lib.get("title") for lib in libraries if lib.get("title") in selected_libraries],
                        asset_path=config["assets_path"],
                    )

                # Final summary report
                elapsed_time = time.time() - start_time
                minutes, seconds = divmod(int(elapsed_time), 60)

                meta_summary_banner(logger)
                logger.info(f"[Summary] Processing completed in {minutes} minutes {seconds} seconds.")

                skipped_libraries = [lib.get("title") for lib in libraries if lib.get("title") not in selected_libraries]
                logger.info(
                    f"[Summary] Libraries processed: {len(library_item_counts)} | "
                    f"skipped: {', '.join(skipped_libraries) if skipped_libraries else 'None'}"
                )
                processed_count = sum(library_item_counts.values())
                logger.info(f"[Summary] Total items processed: {processed_count}")

                logger.info("[Per-Library Metadata Counts]")
                for lib_name, count in library_item_counts.items():
                    logger.info(f"  - {lib_name}: {count} items processed")

                logger.info("[Per-Library Metadata Stats]")
                for lib_name, summary in metadata_summaries.items():
                    logger.info(
                        f"  - {lib_name}: {summary['complete']}/{summary['total_items']} complete, {summary['incomplete']} incomplete"
                        f", {summary['percent_complete']}% complete"
                    )

                logger.info("[Per-Library Downloaded Asset Size]")
                for lib_name, size in library_filesize.items():
                    logger.info(f"  - {lib_name}: {human_readable_size(size)}")
                total_asset_size = sum(library_filesize.values())
                logger.info(f"[Summary] Total assets downloaded: {human_readable_size(total_asset_size)}")

                logger.info("[Total Cleanup Stats]")
                if cleanup_orphans_flag:
                    logger.info(f"  - Titles Removed (Orphans): {orphans_removed}")
                logger.info("=" * 60)

                if config.get("dry_run", False):
                    logger.info("[Dry Run] Completed. No files were written.")
            except Exception as e:
                logger.error(f"[Fatal] Unhandled exception in main: {e}", exc_info=True)
                sys.exit(1)

    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"[Fatal] Unhandled exception: {e}", exc_info=True)
        sys.exit(1)