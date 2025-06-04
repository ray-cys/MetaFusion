import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import RLock
from helper.config import load_config
from helper.tmdb import resolve_tmdb_id, update_tmdb_cache, cache_lock, save_cache, save_failed_cache, tmdb_cache, failed_cache
from helper.stats import summarize_metadata_completeness

config = load_config()

# Locks for thread safety
metadata_lock = RLock()
assets_lock = RLock()
summary_lock = RLock()

def process_item_metadata_and_assets(
    plex_item, 
    consolidated_metadata, 
    dry_run=False, 
    existing_yaml_data=None, 
    library_item_counts=None, 
    library_name="Unknown",
    existing_assets=None,
    library_filesize=None,
    season_cache=None, 
    episode_cache=None,
    movie_cache=None
):
    from modules.media_metadata import build_movie_metadata, build_tv_metadata
    from modules.media_assets import (
        process_poster_for_media, process_season_poster,
        process_background_for_media, process_season_background
    )
    """
    Process a single Plex item: build metadata and download/process poster assets.
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
        logging.info(f"[Dry Run] Would process metadata and assets for: {full_title} ({library_type})")
        return

    logging.debug(f"[Processing] Started processing: {full_title} ({library_type})")

    # Resolve TMDb ID for the item
    tmdb_id = resolve_tmdb_id(plex_item, title, year, library_type)
    if not tmdb_id:
        logging.warning(f"[Metadata] TMDb ID not found for {full_title}. Skipping...")
        return

    # Update cache with TMDb ID
    library_type = library_type.lower()
    if library_type == "show":
        library_type = "tv"
    elif library_type != "movie" and library_type != "tv":
        library_type = "movie"
    cache_key = f"{library_type}:{title}:{year}"
    update_tmdb_cache(cache_key, tmdb_id, title, year, library_type)

    # # Build metadata based on library type (configurable through config)
    if config.get("process_metadata", True):
        try:
            with metadata_lock:
                if library_type == "movie":
                    build_movie_metadata(plex_item, consolidated_metadata, dry_run, existing_yaml_data)
                elif library_type == "tv":
                    build_tv_metadata(plex_item, consolidated_metadata, dry_run, existing_yaml_data, season_cache, episode_cache)
                else:
                    logging.warning(f"[Metadata] Unsupported library type '{library_type}' for {full_title}. Skipping...")
                    return
        except Exception as e:
            logging.error(f"[Processing Error] Failed to process metadata for {full_title}: {e}")
            return
    else:
        logging.info(f"[Config] Metadata processing disabled for {full_title}.")

    # Update item count for the library
    if library_item_counts is not None and library_name != "Unknown":
        with cache_lock:
            library_item_counts[library_name] = library_item_counts.get(library_name, 0) + 1

    # Process poster assets and season posters (configurable through config)
    total_downloaded = 0
    if config.get("process_assets", True):
        try:
            with assets_lock:
                if library_type == "movie":
                    size = process_poster_for_media("movie", tmdb_id, plex_item, library_name, existing_assets, episode_cache, movie_cache)
                    total_downloaded += size
                elif library_type in ["show", "tv"]:
                    size = process_poster_for_media("tv", tmdb_id, plex_item, library_name, existing_assets, episode_cache, movie_cache)
                    total_downloaded += size
                    # --- SEASON POSTER SWITCH ---
                    if config.get("process_season_posters", True):
                        for season in getattr(plex_item, "seasons", lambda: [])():
                            size = process_season_poster(tmdb_id, season.index, plex_item, library_name, existing_assets, episode_cache)
                            total_downloaded += size
        except Exception as e:
            logging.error(f"[Processing Error] Failed to process assets for {full_title}: {e}")
    else:
        logging.info(f"[Config] Asset processing disabled for {full_title}.")

    # Process background assets (configurable through config)
    if config.get("process_backgrounds", True):
        try:
            with assets_lock:
                if library_type == "movie":
                    size = process_background_for_media("movie", tmdb_id, plex_item, library_name, existing_assets, season_cache, movie_cache)
                    total_downloaded += size
                elif library_type in ["show", "tv"]:
                    size = process_background_for_media("tv", tmdb_id, plex_item, library_name, existing_assets, season_cache, movie_cache)
                    total_downloaded += size
                    # Process season background assets (configurable through config)
                    if config.get("process_season_backgrounds", True):
                        for season in getattr(plex_item, "seasons", lambda: [])():
                            size = process_season_background(tmdb_id, season.index, plex_item, library_name, existing_assets, season_cache, movie_cache)
                            total_downloaded += size
        except Exception as e:
            logging.error(f"[Processing Error] Failed to process backgrounds for {full_title}: {e}")
    else:
        logging.info(f"[Config] Background processing disabled for {full_title}.")
        
    if library_filesize is not None and library_name != "Unknown":
        with assets_lock:
            library_filesize[library_name] = library_filesize.get(library_name, 0) + total_downloaded
            
    logging.debug(f"[Processing] Finished processing: {full_title} ({library_type})")

def process_library(
    plex, 
    library_name, 
    dry_run=False, 
    library_item_counts=None, 
    metadata_summaries=None, 
    library_filesize=None, 
    season_cache=None, 
    episode_cache=None,
    movie_cache=None
    ):
    from ruamel.yaml import YAML
    """
    Process all items in a Plex library, build metadata, download assets, and save to YAML.
    """
    output_path = Path(config["metadata_path"]) / f"{library_name.lower().replace(' ', '_')}.yml"
    existing_yaml_data = {}

    # Load existing metadata if present
    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                yaml = YAML()
                existing_yaml_data = yaml.load(f) or {}
        except yaml.YAMLError:
            logging.error(f"[YAML Error] Failed to parse existing metadata file: {output_path}")

    consolidated_metadata = existing_yaml_data if existing_yaml_data else {"metadata": {}}
    existing_assets = set()

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
                    process_item_metadata_and_assets, item, consolidated_metadata, dry_run,
                    existing_yaml_data, library_item_counts, library_name, existing_assets, library_filesize,
                    season_cache, episode_cache, movie_cache,
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
                output_path.parent.mkdir(parents=True, exist_ok=True)
                logging.debug(f"[Metadata] Saving metadata to {output_path}...")
                with open(output_path, "w", encoding="utf-8") as f:
                    yaml = YAML()
                    yaml.default_flow_style = False
                    yaml.allow_unicode = True
                    yaml.dump(consolidated_metadata, f)
                logging.info(f"[Metadata] Metadata successfully saved to {output_path}")

                with cache_lock:
                    logging.debug("[Cache] Saving tmdb_cache and failed_cache...")
                    save_cache(tmdb_cache)
                    save_failed_cache(failed_cache)
                    logging.info("[Cache] Cache save completed.")

            except Exception as e:
                logging.error(f"[File Save Error] Failed to write metadata: {e}")
        else:
            logging.info(f"[Dry Run] Metadata for {library_name} generated but not saved.")

        # Summarize metadata completeness
        is_tv = "tv" in library_name.lower()
        summary = summarize_metadata_completeness(
            library_name,
            output_path,
            total_items,
            is_tv=is_tv,
            ignored_fields={"collections"},  # Example: ignore 'collections'
        )
        if metadata_summaries is not None:
            with summary_lock:
                metadata_summaries[library_name] = summary

        logging.info(f"[Library Summary] Finished processing {library_name}. Total Items: {total_items}")
    except Exception as e:
        logging.error(f"[Library] Failed to process library {library_name}: {e}")
