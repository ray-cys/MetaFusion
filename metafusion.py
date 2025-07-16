import sys, asyncio, aiohttp, time, schedule, argparse
from pathlib import Path
from datetime import datetime
from helper.config import load_config_file, get_disabled_features, get_feature_flags
from helper.plex import connect_plex_library, _plex_cache
from helper.tmdb import tmdb_response_cache
from helper.logging import (
    get_setup_logging, get_meta_banner, check_sys_requirements, log_final_summary, log_main_event
)
from modules.processing import process_library, plex_metadata_dict
from modules.cleanup import cleanup_title_orphans

def parse_cli_args():
    parser = argparse.ArgumentParser(description="MetaFusion CLI Command Overrides")
    parser.add_argument("--metafusion_run", type=str, choices=["true", "false"], help="Run MetaFusion job")
    parser.add_argument("--schedule", type=str, choices=["true", "false"], help="Enable schedule")
    parser.add_argument("--run_times", type=str, help="Comma-separated run times (e.g. 06:00,18:30)")
    parser.add_argument("--dry_run", type=str, choices=["true", "false"], help="Dry run mode")
    parser.add_argument("--mode", type=str, choices=["kometa", "plex"], help="Run mode")
    parser.add_argument("--run_basic", type=str, choices=["true", "false"], help="Run basic metadata extraction")
    parser.add_argument("--run_enhanced", type=str, choices=["true", "false"], help="Run enhanced metadata extraction")
    parser.add_argument("--run_poster", type=str, choices=["true", "false"], help="Run poster asset download")
    parser.add_argument("--run_season", type=str, choices=["true", "false"], help="Run season asset download")
    parser.add_argument("--run_background", type=str, choices=["true", "false"], help="Run background asset download")
    return parser.parse_args()

def override_config_with_cli(config, args):
    if args.metafusion_run is not None:
        config["metafusion_run"] = args.metafusion_run.lower() == "true"
    if args.schedule is not None:
        config["settings"]["schedule"] = args.schedule.lower() == "true"
    if args.run_times is not None:
        config["settings"]["run_times"] = [t.strip() for t in args.run_times.split(",") if t.strip()]
    if args.dry_run is not None:
        config["settings"]["dry_run"] = args.dry_run.lower() == "true"
    if args.mode is not None:
        config["settings"]["mode"] = args.mode
    if args.run_basic is not None:
        config["metadata"]["run_basic"] = args.run_basic.lower() == "true"
    if args.run_enhanced is not None:
        config["metadata"]["run_enhanced"] = args.run_enhanced.lower() == "true"
    if args.run_poster is not None:
        config["assets"]["run_poster"] = args.run_poster.lower() == "true"
    if args.run_season is not None:
        config["assets"]["run_season"] = args.run_season.lower() == "true"
    if args.run_background is not None:
        config["assets"]["run_background"] = args.run_background.lower() == "true"
        
args = parse_cli_args()
config = load_config_file()
override_config_with_cli(config, args)
logger = get_setup_logging(config)

async def metafusion_main():
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
        sections, selected_libraries, all_libraries = connect_plex_library(config)
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

        if tasks:
            await asyncio.gather(*tasks)
        else:
            log_main_event("main_no_libraries")

        orphans_removed = 0
        if feature_flags.get("cleanup", False):
            kometa_root = config.get("settings", {}).get("path", ".")
            asset_path = Path(kometa_root) / "assets"
            orphans_removed = await cleanup_title_orphans(
                config=config, asset_path=asset_path,
                preloaded_plex_metadata=plex_metadata_dict, feature_flags=feature_flags
            )

        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        log_final_summary(
            logger, elapsed_time, library_item_counts, metadata_summaries, library_filesize,
            orphans_removed, cleanup_title_orphans, selected_libraries, all_libraries, config,
            feature_flags=feature_flags
        )
    _plex_cache.clear()
    plex_metadata_dict.clear()
    tmdb_response_cache.clear()

def run_metafusion_job():
    try:
        asyncio.run(metafusion_main())
    except Exception as e:
        log_main_event("main_unhandled_exception", error=e)
        sys.exit(1)

if __name__ == "__main__":
    settings = config.get("settings", {})
    run_times = settings.get("run_times", [])
    schedule_enabled = settings.get("schedule", False)
    metafusion_run = config.get("metafusion_run", True)

    if metafusion_run:
        run_metafusion_job()
        log_main_event("main_force_run", start_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    elif schedule_enabled and run_times:
        for t in run_times:
            schedule.every().day.at(t).do(run_metafusion_job)
        log_main_event("main_scheduled_run", run_time=', '.join(run_times))
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        log_main_event("main_processing_disabled", logger=logger)