import os
import logging
import yaml
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import Lock

from helper.tmdb import resolve_tmdb_id, update_tmdb_cache, cache_lock, save_cache, save_failed_cache, tmdb_cache, failed_items_cache
from helper.config import load_config
from modules.media_metadata import build_movie_metadata, build_tv_metadata
from modules.media_assets import process_poster_for_media, process_season_poster

config = load_config()
METADATA_DIR = Path(config["metadata_path"])

# Locks for thread safety
metadata_lock = Lock()
assets_lock = Lock()
summary_lock = Lock()

def process_item_metadata_and_assets(
    plex_item, 
    consolidated_metadata, 
    dry_run=False, 
    existing_yaml_data=None, 
    library_item_counts=None, 
    library_name="Unknown",
    existing_assets=None,
    summary=None
):
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
    update_tmdb_cache(full_title, tmdb_id, title, year, library_type)

    try:
        # Build metadata based on library type
        with metadata_lock:
            if library_type == "movie":
                build_movie_metadata(plex_item, consolidated_metadata, dry_run, existing_yaml_data)
            elif library_type in ["show", "tv"]:
                build_tv_metadata(plex_item, consolidated_metadata, dry_run, existing_yaml_data)
            else:
                logging.warning(f"[Metadata] Unsupported library type '{library_type}' for {full_title}. Skipping...")
                return
    except Exception as e:
        logging.error(f"[Processing Error] Failed to process metadata for {full_title}: {e}")
        return

    # Update item count for the library
    if library_item_counts is not None and library_name != "Unknown":
        with cache_lock:
            library_item_counts[library_name] = library_item_counts.get(library_name, 0) + 1

    # Process poster assets
    try:
        with assets_lock:
            if library_type == "movie":
                process_poster_for_media("movie", tmdb_id, plex_item, library_name, existing_assets, summary)
            elif library_type in ["show", "tv"]:
                process_poster_for_media("tv", tmdb_id, plex_item, library_name, existing_assets, summary)
                # Process season posters
                for season in getattr(plex_item, "seasons", lambda: [])():
                    process_season_poster(tmdb_id, season.index, plex_item, library_name, existing_assets, summary)
    except Exception as e:
        logging.error(f"[Processing Error] Failed to process assets for {full_title}: {e}")

    logging.debug(f"[Processing] Finished processing: {full_title} ({library_type})")

def process_library(plex, library_name, dry_run=False, library_item_counts=None):
    """
    Process all items in a Plex library, build metadata, download assets, and save to YAML.
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
    existing_assets = set()
    summary = {"downloaded": 0, "updated": 0, "skipped": 0}

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
                    existing_yaml_data, library_item_counts, library_name, existing_assets, summary
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
        logging.info(f"[Library Summary] Assets Downloaded: {summary['downloaded']}, Updated: {summary['updated']}, Skipped: {summary['skipped']}")
    except Exception as e:
        logging.error(f"[Library] Failed to process library {library_name}: {e}")