import logging
import asyncio
from pathlib import Path
from helper.config import load_config
from helper.cache import save_cache, save_failed_cache, load_cache, load_failed_cache
from helper.plex import plex_metadata

config = load_config()

async def process_item(
    plex_item, 
    consolidated_metadata, 
    dry_run=False, 
    existing_yaml_data=None, 
    library_name="Unknown",
    existing_assets=None,
    session=None,
    ignored_fields=None,
):
    from modules.builder import build_movie, build_tv
    """
    Process a single Plex item: build metadata and download/process assets.
    """
    if ignored_fields is None:
        ignored_fields = set()
    if not plex_item:
        logging.warning("[Plex] No item found. Skipping...")
        return None

    meta = await plex_metadata(plex_item)
    title = meta.get("title", "Unknown")
    year = meta.get("year", "Unknown")
    full_title = f"{title} ({year})"
    
    # Determine library name and type
    if library_name == "Unknown":
        library_name = meta.get("library_name", "Unknown")
    library_type = meta.get("library_type", "unknown")

    if dry_run:
        logging.info(f"[Dry Run] Would process metadata and assets for: {full_title} ({library_type})")
        return None

    logging.debug(f"[{library_type}] Started processing: {full_title}")

    try:
        async with asyncio.Lock():
            if library_type == "movie":
                stats = await build_movie(
                    plex_item, consolidated_metadata, dry_run,
                    existing_yaml_data=existing_yaml_data, session=session,
                    ignored_fields=ignored_fields, existing_assets=existing_assets,
                    library_name=library_name, meta=meta
                )
            elif library_type in ("show", "tv"):
                stats = await build_tv(
                    plex_item, consolidated_metadata, dry_run,
                    existing_yaml_data=existing_yaml_data, session=session,
                    ignored_fields=ignored_fields, existing_assets=existing_assets,
                    library_name=library_name, meta=meta
                )
            else:
                logging.warning(f"[{library_type}] Unsupported library type for {full_title}. Skipping...")
                return None
    except Exception as e:
        logging.error(f"[{library_type}] Failed to process {full_title}: {e}")
        return None

    logging.debug(f"[{library_type}] Finished processing: {full_title}")
    return stats 

async def process_library(
    library_section,
    dry_run=False, 
    library_item_counts=None, 
    library_filesize=None, 
    metadata_summaries=None, 
    season_cache=None, 
    episode_cache=None,
    movie_cache=None,
    session=None,
    ignored_fields=None,
):
    from ruamel.yaml import YAML
    from helper.logging import human_readable_size
    """
    Processes all items in a given Plex library section.
    """
    library_name = library_section.title
    if ignored_fields is None:
        ignored_fields = {"collection", "guest"}
    output_path = Path(config["metadata"]["directory"]) / f"{library_name.lower().replace(' ', '_')}.yml"
    existing_yaml_data = {}
    
    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                yaml = YAML()
                existing_yaml_data = yaml.load(f) or {}
        except Exception as e:
            logging.error(f"[YAML] Failed to parse existing metadata file: {output_path} ({e})")

    consolidated_metadata = existing_yaml_data if existing_yaml_data else {"metadata": {}}
    existing_assets = set()

    try:
        library_name = library_section.title
        items = await asyncio.to_thread(library_section.all)
        total_items = len(items)
        logging.info(f"[{library_name}] Processing library with {total_items} items.")

        for item in items:
            await plex_metadata(
                item, 
                _season_cache=season_cache, 
                _episode_cache=episode_cache, 
                _movie_cache=movie_cache
            )

        # Initialize aggregation structures
        if library_item_counts is not None:
            library_item_counts[library_name] = 0
        if library_filesize is not None:
            library_filesize[library_name] = 0

        total_asset_size = 0
        completed = 0
        incomplete = 0

        async def process_and_collect(item):
            stats = await process_item(
                item, consolidated_metadata, dry_run, existing_yaml_data,
                library_name, existing_assets, session=session, ignored_fields=ignored_fields
            )
            # Aggregate asset sizes
            if stats and isinstance(stats, dict):
                for key in ("poster", "season_poster", "background", "collection_poster", "collection_background"):
                    total = stats.get(key, {}).get("size", 0)
                    nonlocal total_asset_size
                    total_asset_size += total
                # Completion tracking
                percent = stats.get("percent", 0)
                if percent >= 100:
                    nonlocal completed
                    completed += 1
                else:
                    nonlocal incomplete
                    incomplete += 1
            # Aggregate item count
            if library_item_counts is not None and library_name != "Unknown":
                library_item_counts[library_name] = library_item_counts.get(library_name, 0) + 1

        # Process each item asynchronously
        item_tasks = [process_and_collect(item) for item in items]
        await asyncio.gather(*item_tasks)

        # Save total asset size for this library
        if library_filesize is not None:
            library_filesize[library_name] = total_asset_size

        # Save metadata and caches
        if not dry_run:
            try:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                logging.debug(f"[{library_name}] Saving metadata to {output_path}...")
                with open(output_path, "w", encoding="utf-8") as f:
                    yaml = YAML()
                    yaml.default_flow_style = False
                    yaml.allow_unicode = True
                    yaml.dump(consolidated_metadata, f)
                logging.debug(f"[{library_name}] Metadata successfully saved to {output_path}")

                # Save cache and failed cache using always-fresh loads
                logging.debug("[Cache] Saving cache files...")
                save_cache(load_cache())
                save_failed_cache(load_failed_cache())
                logging.debug("[Cache] Cache files saved.")

            except Exception as e:
                logging.error(f"[{library_name}] Failed to write metadata: {e}")
        else:
            logging.info(f"[Dry Run] Metadata for {library_name} generated but not saved.")

        # In-memory summary
        percent_complete = round((completed / total_items) * 100, 2) if total_items else 0.0
        logging.info(
            f"[Summary] '{library_name}' Metadata: {completed}/{total_items} completed, {incomplete} incomplete ({percent_complete}%)"
        )
        if library_filesize is not None:
            logging.info(
                f"[Summary] '{library_name}' Assets: total {human_readable_size(library_filesize[library_name])} downloaded"
            )
        if metadata_summaries is not None:
            metadata_summaries[library_name] = {
                "complete": completed,
                "incomplete": incomplete,
                "total_items": total_items,
                "percent_complete": percent_complete,
            }

        logging.info(f"[Summary] '{library_name}' Items: total processed {total_items} titles")
    except Exception as e:
        logging.error(f"[Summary] Failed to process library '{library_name}': {e}")