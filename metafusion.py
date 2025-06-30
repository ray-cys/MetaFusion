import sys, asyncio, aiohttp
from datetime import datetime
from helper.config import load_config_file, get_disabled_features, get_feature_flags
from helper.logging import (
    get_setup_logging, get_meta_banner, check_sys_requirements, log_final_summary, log_main_event
)
from helper.plex import connect_plex_library
from modules.processing import process_library, plex_metadata_dict
from modules.cleanup import cleanup_title_orphans

config = load_config_file()
logger = get_setup_logging(config)

if __name__ == "__main__":
    async def main():
        if not config.get("metafusion_run", True):
            log_main_event("main_processing_disabled")
            return

        get_meta_banner(logger)
        check_sys_requirements(logger, config=config)
        log_main_event(
            "main_started", start_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        get_disabled_features(config, logger)
        feature_flags = get_feature_flags(config)
        start_time = datetime.now()
        library_item_counts = {}

        async with aiohttp.ClientSession() as session:
            plex, sections, libraries, selected_libraries, all_libraries = connect_plex_library(config)
            metadata_summaries = {}
            library_filesize = {}

            tasks = []
            for section in sections:
                movie_cache = {}
                season_cache = {}
                episode_cache = {}

                tasks.append(
                    process_library(
                        library_section=section, config=config, library_item_counts=library_item_counts,
                        metadata_summaries=metadata_summaries, library_filesize=library_filesize,
                        season_cache=season_cache, episode_cache=episode_cache, movie_cache=movie_cache,
                        session=session, feature_flags=feature_flags
                    )
                )

            results = []
            if tasks:
                results = await asyncio.gather(*tasks)
            else:
                log_main_event("main_no_libraries")

            orphans_removed = 0
            if feature_flags.get("cleanup", False):
                orphans_removed = await cleanup_title_orphans(
                    config=config, asset_path=config["assets"]["path"],
                    preloaded_plex_metadata=plex_metadata_dict, feature_flags=feature_flags
                )

            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            log_final_summary(
                logger, elapsed_time, library_item_counts, metadata_summaries, library_filesize,
                orphans_removed, cleanup_title_orphans, selected_libraries, all_libraries, config,
                feature_flags=feature_flags
            )

    try:
        asyncio.run(main())
    except Exception as e:
        log_main_event("main_unhandled_exception", error=e)
        sys.exit(1)