import logging
import asyncio
from pathlib import Path
from helper.config import load_config
from helper.tmdb import resolve_tmdb_id, update_meta_cache
from helper.cache import save_cache, save_failed_cache, load_cache, load_failed_cache
from helper.stats import summarize_metadata_completeness

config = load_config()

async def process_item_metadata_and_assets_async(
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
    movie_cache=None,
    asset_stats=None,
    run_upgrade=True,
    session=None,
    ignored_fields=None,
):
    from modules.media_metadata import build_movie_metadata, build_tv_metadata
    from modules.media_assets import (
        process_poster_for_media, process_season_poster,
        process_background_for_media
    )
    if ignored_fields is None:
        ignored_fields = set()
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

    # Resolve TMDb ID for the item (await async function)
    tmdb_id = await resolve_tmdb_id(plex_item, title, year, library_type, session=session)
    if not tmdb_id:
        logging.warning(f"[Metadata] TMDb ID not found for {full_title}. Skipping...")
        return

    # Update cache with TMDb ID (no global cache variable used)
    library_type = library_type.lower()
    if library_type == "show":
        library_type = "tv"
    elif library_type != "movie" and library_type != "tv":
        library_type = "movie"
    cache_key = f"{library_type}:{title}:{year}"
    update_meta_cache(cache_key, tmdb_id, title, year, library_type)

    # Build metadata based on library type (configurable through config)
    if config.get("process_metadata", True):
        try:
            async with asyncio.Lock():
                if library_type == "movie":
                    await build_movie_metadata(
                        plex_item, consolidated_metadata, dry_run,
                        run_upgrade=run_upgrade, existing_yaml_data=existing_yaml_data, session=session,
                        ignored_fields=ignored_fields 
                    )
                elif library_type == "tv":
                    await build_tv_metadata(
                        plex_item, consolidated_metadata, dry_run,
                        run_upgrade=run_upgrade, existing_yaml_data=existing_yaml_data,
                        season_cache=season_cache, episode_cache=episode_cache, session=session,
                        ignored_fields=ignored_fields
                    )
                else:
                    logging.warning(f"[Metadata] Unsupported library type '{library_type}' for {full_title}. Skipping...")
                    return
        except Exception as e:
            logging.error(f"[Processing Error] Failed to process metadata for {full_title}: {e}")
            return
    else:
        logging.debug(f"[Config] Metadata processing disabled for {full_title}.")

    # Initialize item count for the library
    if library_item_counts is not None and library_name != "Unknown":
        library_item_counts[library_name] = library_item_counts.get(library_name, 0) + 1

    # Initialize asset stats for the library
    if asset_stats is None:
        asset_stats = {"poster": {"count": 0, "size": 0},
                       "season_poster": {"count": 0, "size": 0},
                       "background": {"count": 0, "size": 0}}

    # Process poster assets and season posters
    if config.get("process_posters", True):
        try:
            async with asyncio.Lock():
                if library_type == "movie":
                    size, count = await process_poster_for_media(
                        "movie", tmdb_id, plex_item, library_name, existing_assets,
                        episode_cache, movie_cache, session=session
                    )
                    asset_stats["poster"]["size"] += size
                    asset_stats["poster"]["count"] += count
                elif library_type in ["show", "tv"]:
                    size, count = await process_poster_for_media(
                        "tv", tmdb_id, plex_item, library_name, existing_assets,
                        episode_cache, movie_cache, session=session
                    )
                    asset_stats["poster"]["size"] += size
                    asset_stats["poster"]["count"] += count
                    if config.get("process_season_posters", True):
                        for season in getattr(plex_item, "seasons", lambda: [])():
                            size, count = await process_season_poster(
                                tmdb_id, season.index, plex_item, library_name, existing_assets,
                                episode_cache, session=session
                            )
                            asset_stats["season_poster"]["size"] += size
                            asset_stats["season_poster"]["count"] += count
        except Exception as e:
            logging.error(f"[Processing Error] Failed to process assets for {full_title}: {e}")
    else:
        logging.debug(f"[Config] Asset processing disabled for {full_title}.")

    # Process background assets
    if config.get("process_backgrounds", True):
        try:
            async with asyncio.Lock():
                if library_type == "movie":
                    size, count = await process_background_for_media(
                        "movie", tmdb_id, plex_item, library_name, existing_assets,
                        season_cache, movie_cache, session=session
                    )
                    asset_stats["background"]["size"] += size
                    asset_stats["background"]["count"] += count
                elif library_type in ["show", "tv"]:
                    size, count = await process_background_for_media(
                        "tv", tmdb_id, plex_item, library_name, existing_assets,
                        season_cache, movie_cache, session=session
                    )
                    asset_stats["background"]["size"] += size
                    asset_stats["background"]["count"] += count
        except Exception as e:
            logging.error(f"[Processing Error] Failed to process backgrounds for {full_title}: {e}")
    else:
        logging.debug(f"[Config] Background processing disabled for {full_title}.")
    
    # Track total assets filesize downloaded for the library
    if library_filesize is not None and library_name != "Unknown":
        total_asset_size = (
            asset_stats["poster"]["size"] +
            asset_stats["season_poster"]["size"] +
            asset_stats["background"]["size"]
        )
        library_filesize[library_name] = library_filesize.get(library_name, 0) + total_asset_size
            
    logging.debug(f"[Processing] Finished processing: {full_title} ({library_type})")
    return asset_stats

async def process_library_async(
    plex, 
    library_name, 
    dry_run=False, 
    library_item_counts=None, 
    metadata_summaries=None, 
    library_filesize=None, 
    season_cache=None, 
    episode_cache=None,
    movie_cache=None,
    run_upgrade=True,
    session=None,
    ignored_fields=None,
    ):
    from ruamel.yaml import YAML
    if ignored_fields is None:
        ignored_fields = {"collections"}
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
        except Exception as e:
            logging.error(f"[YAML Error] Failed to parse existing metadata file: {output_path} ({e})")

    consolidated_metadata = existing_yaml_data if existing_yaml_data else {"metadata": {}}
    existing_assets = set()

    try:
        library = plex.library.section(library_name)
        items = await asyncio.to_thread(library.all)
        total_items = len(items)
        logging.info(f"[Library] Processing library: {library_name} with {total_items} items.")

        if library_item_counts is not None:
            library_item_counts.setdefault(library_name, 0)

        asset_stats = {"poster": {"count": 0, "size": 0},
                    "season_poster": {"count": 0, "size": 0},
                    "background": {"count": 0, "size": 0}}

        # Process each item asynchronously
        item_tasks = [
            process_item_metadata_and_assets_async(
                item, consolidated_metadata, dry_run, existing_yaml_data, library_item_counts,
                library_name, existing_assets, library_filesize, season_cache, episode_cache, movie_cache,
                asset_stats, run_upgrade=run_upgrade, session=session, ignored_fields=ignored_fields
            )
            for item in items
        ]
        await asyncio.gather(*item_tasks)

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
                logging.debug(f"[Metadata] Metadata successfully saved to {output_path}")

                # Save cache and failed cache using always-fresh loads
                logging.debug("[Cache] Saving meta_cache and failed_cache...")
                save_cache(load_cache())
                save_failed_cache(load_failed_cache())
                logging.debug("[Cache] Cache save completed.")

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
            ignored_fields=ignored_fields,
            asset_stats=asset_stats
        )
        if metadata_summaries is not None:
            metadata_summaries[library_name] = summary

        logging.debug(f"[Library] Finished processing {library_name}. Total Items: {total_items}")
    except Exception as e:
        logging.error(f"[Library] Failed to process library {library_name}: {e}")