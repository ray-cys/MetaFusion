import sys
import asyncio
import aiohttp
from datetime import datetime
from helper.config import load_config_file, get_disabled_features
from helper.logging import (
    get_setup_logging, get_meta_banner, check_sys_requirements, log_final_summary, log_helper_event
)
from helper.plex import connect_plex_library
from modules.processing import process_library, plex_metadata_dict
from modules.cleanup import cleanup_title_orphans

config = load_config_file()
logger = get_setup_logging(config)

if __name__ == "__main__":
    async def main():
        if not config.get("metafusion_run", True):
            log_helper_event("metafusion_processing_disabled")
            return

        get_meta_banner(logger)
        log_helper_event(
            "metafusion_started", start_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        check_sys_requirements(logger, config=config)
        get_disabled_features(config, logger)
        start_time = datetime.now()
        library_item_counts = {}

        log_helper_event("metafusion_processing_metadata")
        async with aiohttp.ClientSession() as session:
            plex, sections, libraries, selected_libraries = connect_plex_library(config)
            metadata_summaries = {}
            library_filesize = {}

            collection_asset_paths = set()
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
                        session=session,
                    )
                )

            results = []
            if tasks:
                results = await asyncio.gather(*tasks)
            else:
                log_helper_event("metafusion_no_libraries")
            
            for result in results:
                if isinstance(result, tuple) and len(result) > 1:
                    _, section_collection_asset_paths, section_orphans_removed = result
                    collection_asset_paths.update(section_collection_asset_paths)

            orphans_removed = 0
            if config["cleanup"].get("run_process", True):
                orphans_removed = await cleanup_title_orphans(
                    config=config, libraries=[library_name], asset_path=config["assets"]["path"],
                    preloaded_plex_metadata=plex_metadata_dict,
                    valid_collection_assets=collection_asset_paths,
                )

            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            log_final_summary(
                logger, elapsed_time, library_item_counts, metadata_summaries, library_filesize,
                orphans_removed, cleanup_title_orphans, selected_libraries, libraries,
                config=config,
            )

    try:
        asyncio.run(main())
    except Exception as e:
        log_helper_event("metafusion_unhandled_exception", error=e)
        sys.exit(1)