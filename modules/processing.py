import os
import logging
import yaml
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

from helper.tmdb import resolve_tmdb_id, update_tmdb_cache, cache_lock, save_cache, save_failed_cache, tmdb_cache, failed_items_cache
from helper.config import load_config
from modules.media_metadata import build_movie_metadata, build_tv_metadata
from modules.media_assets import process_poster_for_media, process_season_poster

config = load_config()
METADATA_DIR = Path(config["metadata_path"])

def process_plex_item(
    plex_item, 
    consolidated_metadata, 
    dry_run=False, 
    existing_yaml_data=None, 
    library_item_counts=None, 
    library_name="Unknown",
):
    """
    Process a single Plex item (movie or TV show), build metadata, and update counts.

    Args:
        plex_item: The Plex item object.
        consolidated_metadata (dict): The dictionary to store consolidated metadata.
        dry_run (bool): If True, only simulate the operation.
        existing_yaml_data (dict, optional): Existing YAML metadata for smart update.
        library_item_counts (dict, optional): Dictionary to track item counts per library.
        library_name (str): The name of the Plex library.

    Returns:
        None
    """
    if not plex_item:
        logging.warning("[Plex] plex_item is None. Skipping item.")
        return

    title = getattr(plex_item, "title", "Unknown")
    year = getattr(plex_item, "year", "Unknown")
    full_title = f"{title} ({year})"

    # Determine library name and type
    if library_name == "Unknown":
        library_section = getattr(plex_item, "librarySection", None)
        library_name = getattr(library_section, "title", "Unknown") if library_section else "Unknown"

    library_section = getattr(plex_item, "librarySection", None)
    library_type = getattr(library_section, "type", "Unknown").lower() if library_section else "unknown"
    if library_type == "unknown":
        library_type = getattr(plex_item, "type", "Unknown").lower()

    if dry_run:
        logging.info(f"[Dry Run] Would process metadata for: {full_title} ({library_type})")
        return

    logging.debug(f"[Processing] Started processing: {full_title} ({library_type})")

    # Resolve TMDb ID for the item
    tmdb_id = resolve_tmdb_id(plex_item, library_type)
    if not tmdb_id:
        logging.warning(f"[Metadata] TMDb ID not found for {full_title}. Skipping...")
        return

    # Update cache with TMDb ID
    update_tmdb_cache(full_title, tmdb_id, title, year, library_type)

    try:
        # Build metadata based on library type
        if library_type == "movie":
            build_movie_metadata(plex_item, consolidated_metadata, dry_run, existing_yaml_data)
        elif library_type in ["show", "tv"]:
            build_tv_metadata(plex_item, consolidated_metadata, dry_run, existing_yaml_data)
        else:
            logging.warning(f"[Metadata] Unsupported library type '{library_type}' for {full_title}. Skipping...")
            return
    except Exception as e:
        logging.error(f"[Processing Error] Failed to process {full_title}: {e}")
        return

    # Update item count for the library
    if library_item_counts is not None and library_name != "Unknown":
        with cache_lock:
            library_item_counts[library_name] = library_item_counts.get(library_name, 0) + 1

    logging.debug(f"[Processing] Finished processing: {full_title} ({library_type})")

def process_library(plex, library_name, dry_run=False, library_item_counts=None):
    """
    Process all items in a Plex library, build metadata, and save to YAML.

    Args:
        plex: The Plex server object.
        library_name (str): The name of the Plex library.
        dry_run (bool): If True, only simulate the operation.
        library_item_counts (dict, optional): Dictionary to track item counts per library.

    Returns:
        None
    """
    output_path = METADATA_DIR / f"{library_name.lower().replace(' ', '_')}.yml"
    existing_yaml_data = {}

    # Load existing metadata if present
    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing_yaml_data = yaml.safe_load(f) or {}
        except yaml.YAMLError:
            logging.error(f"[YAML Error] Failed to parse existing metadata file: {output_path}")

    consolidated_metadata = existing_yaml_data if existing_yaml_data else {"metadata": {}}

    try:
        library = plex.library.section(library_name)
        items = library.all()
        total_items = len(items)
        logging.info(f"[Library] Processing library: {library_name} with {total_items} items.")

        if library_item_counts is not None:
            with cache_lock:
                library_item_counts.setdefault(library_name, 0)

        max_workers = config.get("threads", {}).get("max_workers", 5)
        timeout_seconds = config.get("threads", {}).get("timeout", 300) 

        # Process items concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    process_plex_item, item, consolidated_metadata, dry_run,
                    existing_yaml_data, library_item_counts, library_name
                ): item for item in items
            }

            try:
                for future in as_completed(futures, timeout=timeout_seconds):
                    item = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        item_title = getattr(item, "title", "Unknown")
                        logging.error(f"[Processing Error] Failed to process {item_title}: {e}")
            except TimeoutError:
                logging.error(f"[Timeout] Processing threads did not complete within {timeout_seconds} seconds.")

        # Save metadata and caches if not a dry run
        if not dry_run:
            try:
                logging.debug(f"[Metadata] Saving metadata to {output_path}...")
                with open(output_path, "w", encoding="utf-8") as f:
                    yaml.dump(
                        consolidated_metadata,
                        f,
                        allow_unicode=True,
                        default_flow_style=False,
                        sort_keys=False,
                        Dumper=yaml.SafeDumper
                    )
                logging.info(f"[Metadata] Metadata successfully saved to {output_path}")

                with cache_lock:
                    logging.debug("[Cache] Saving tmdb_cache and failed_items_cache...")
                    save_cache(tmdb_cache)
                    save_failed_cache(failed_items_cache)
                    logging.info("[Cache] Cache save completed.")

            except Exception as e:
                logging.error(f"[File Save Error] Failed to write metadata: {e}")
        else:
            logging.info(f"[Dry Run] Metadata for {library_name} generated but not saved.")

        logging.info(f"[Library Summary] Finished processing {library_name}. Total Items: {total_items}")
    except Exception as e:
        logging.error(f"[Library] Failed to process library {library_name}: {e}")

def process_poster_task_wrapper(*args):
    """
    Wrapper function to process poster tasks for movies, TV shows, or seasons.

    Args:
        *args: Arguments specifying the task type and parameters.

    Returns:
        None
    """
    if not args:
        logging.warning("[Poster Processor] No arguments passed to process_poster_task_wrapper.")
        return

    task_type = args[0]

    try:
        if task_type == "season":
            # Process season poster
            if len(args) != 7:
                logging.warning(f"[Poster Processor] Incorrect arguments for season task: {args}")
                return
            _, tmdb_id, item, season_number, library_name, existing_assets, summary = args
            logging.debug(f"[Poster Processor] Processing season poster: TMDb ID={tmdb_id}, Season={season_number}, Library={library_name}")
            process_season_poster(tmdb_id, season_number, item, library_name, existing_assets, summary)

        elif task_type in ["movie", "tv"]:
            # Process movie or TV show poster
            if len(args) != 6:
                logging.warning(f"[Poster Processor] Incorrect arguments for poster task: {args}")
                return
            media_type, tmdb_id, item, library_name, existing_assets, summary = args
            logging.debug(f"[Poster Processor] Processing {media_type} poster: TMDb ID={tmdb_id}, Library={library_name}")
            process_poster_for_media(media_type, tmdb_id, item, library_name, existing_assets, summary)

        else:
            logging.warning(f"[Poster Processor] Unknown task type received in poster task wrapper: {task_type}")

    except Exception as e:
        logging.error(f"[Poster Processor] Exception occurred while processing poster task: {e}", exc_info=True)

def process_library_assets(plex, summary):
    """
    Process and download poster assets for all items in preferred libraries.

    Args:
        plex: The Plex server object.
        summary (dict): Dictionary for tracking summary statistics.

    Returns:
        set: Set of asset paths that were processed.
    """
    logging.debug("[Script State] Entering process_library_assets")
    existing_assets = set()

    if summary is None:
        summary = {"downloaded": 0, "updated": 0, "skipped": 0}

    optimal_threads = config["assets"].get("thread_count", min(32, os.cpu_count() * 4))
    logging.debug(f"[Thread Pool Executor] Using {optimal_threads} threads as defined in config.")

    for lib in config["preferred_libraries"]:
        poster_processing_tasks = []
        section = plex.library.section(lib)
        library_name = lib
        logging.info(f"[Plex Library] Processing Library: {lib}")

        items = section.all()

        def prepare_poster_tasks(item):
            """
            Prepare poster processing tasks for a Plex item (movie or TV show).

            Args:
                item: The Plex item.

            Returns:
                list: List of task tuples for poster processing.
            """
            title = item.title
            year = item.year
            media_type = "movie" if getattr(item, "TYPE", None) == "movie" else "tv"
            tmdb_guid = getattr(item, 'guid', None)
            tmdb_id = None

            # Try to extract TMDb ID from GUIDs
            for guid in getattr(item, "guids", []):
                if "tmdb" in guid.id:
                    tmdb_id = guid.id.split("://")[1].split("?")[0]
                    break

            if not tmdb_id and tmdb_guid and "tmdb" in tmdb_guid:
                try:
                    tmdb_id = tmdb_guid.split("://")[1].split("?")[0]
                except Exception as e:
                    logging.warning(f"[Plex Library] Failed to parse TMDb ID for {title} ({year}): {e}")

            # Fallback: resolve TMDb ID if not found
            if not tmdb_id:
                tmdb_id = resolve_tmdb_id(item, title, year, media_type)
                if not tmdb_id:
                    return []

            tasks = [(media_type, tmdb_id, item, library_name, existing_assets, summary)]
            # For TV shows, add season poster tasks
            if media_type == "tv":
                for season in item.seasons():
                    season_number = season.index
                    tasks.append((
                        "season", tmdb_id, item, season_number, library_name, existing_assets, summary
                    ))
            return tasks

        # Inline execute_tasks_concurrently for poster processing
        def execute_tasks_concurrently(tasks, task_function, max_workers=None):
            """
            Execute poster processing tasks concurrently.

            Args:
                tasks (list): List of task argument tuples.
                task_function (callable): Function to execute for each task.
                max_workers (int, optional): Number of worker threads.

            Returns:
                None
            """
            if not tasks:
                logging.info("[Thread Pool Executor] No tasks to execute.")
                return

            if max_workers is None:
                max_workers = config["assets"].get("thread_count", min(32, os.cpu_count() * 4))
            logging.debug(
                f"[Thread Pool Executor] Starting concurrent execution for {len(tasks)} tasks with {max_workers} workers. (Dry-run: {config.get('dry_run', False)})"
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(task_function, *task_args) for task_args in tasks]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.warning(f"[Thread Pool Executor] Task execution failed: {e}", exc_info=True)

            logging.debug("[Thread Pool Executor] All concurrent tasks completed.")

        # Prepare poster processing tasks for all items in the library
        with ThreadPoolExecutor(max_workers=optimal_threads) as executor:
            futures = [executor.submit(prepare_poster_tasks, item) for item in items]
            for future in as_completed(futures):
                poster_processing_tasks.extend(future.result())

        # Execute poster processing tasks concurrently
        if poster_processing_tasks:
            logging.debug(
                f"[Thread Pool Executor] Executing {len(poster_processing_tasks)} tasks for library [{lib}] "
                f"with {optimal_threads} workers. (Dry-run: {config.get('dry_run', False)})"
            )
            execute_tasks_concurrently(
                poster_processing_tasks,
                process_poster_task_wrapper,
                max_workers=optimal_threads
            )

        # Log summary for the library
        logging.info("=" * 60)
        logging.info(f"[{lib}] Summary → Downloaded: {summary['downloaded']}")
        logging.info(f"[{lib}] Summary → Updated: {summary['updated']}")
        logging.info(f"[{lib}] Summary → Skipped: {summary['skipped']}")
        logging.info("=" * 60)

    # Log overall summary for all libraries
    logging.info(
        f"[Libraries Summary] → Downloaded: {summary['downloaded']} | "
        f"Updated: {summary['updated']} | Skipped: {summary['skipped']}"
    )
    logging.info("=" * 60)

    return existing_assets