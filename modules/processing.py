import logging
import asyncio
from pathlib import Path
from helper.logging import log_processing_event, log_library_summary, human_readable_size
from helper.cache import save_cache, load_cache
from helper.plex import get_plex_metadata

async def process_item(
    plex_item, 
    consolidated_metadata, 
    config,
    dry_run=False, 
    existing_yaml_data=None, 
    library_name="Unknown",
    existing_assets=None,
    session=None,
    ignored_fields=None,
):
    from modules.builder import build_movie, build_tv

    if ignored_fields is None:
        ignored_fields = set()
    if not plex_item:
        log_processing_event("processing_no_item")
        return None

    meta = await get_plex_metadata(plex_item)
    title = meta.get("title", "Unknown")
    year = meta.get("year", "Unknown")
    full_title = f"{title} ({year})"

    if library_name == "Unknown":
        library_name = meta.get("library_name", "Unknown")
    library_type = meta.get("library_type", "unknown")

    if dry_run:
        log_processing_event("processing_dry_run", full_title=full_title, library_name=library_name)
        return None
    log_processing_event("processing_started", full_title=full_title)

    try:
        async with asyncio.Lock():
            if library_type == "movie":
                stats, collection_asset_paths = await build_movie(
                    config, consolidated_metadata, dry_run,
                    existing_yaml_data=existing_yaml_data, session=session,
                    ignored_fields=ignored_fields, existing_assets=existing_assets,
                    library_name=library_name, meta=meta
                )
            elif library_type in ("show", "tv"):
                stats = await build_tv(
                    config, consolidated_metadata, dry_run,
                    existing_yaml_data=existing_yaml_data, session=session,
                    ignored_fields=ignored_fields, existing_assets=existing_assets,
                    library_name=library_name, meta=meta
                )
            else:
                log_processing_event("processing_unsupported_type", full_title=full_title)
                return None
    except Exception as e:
        log_processing_event("processing_failed", full_title=full_title, error=str(e))
        return None
    log_processing_event("processing_finished", full_title=full_title)
    return stats

async def process_library(
    library_section,
    config,
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

    all_collection_asset_paths = set()
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
            log_processing_event("processing_failed_parse_yaml", output_path=output_path, error=str(e))

    consolidated_metadata = existing_yaml_data if existing_yaml_data else {"metadata": {}}
    existing_assets = set()

    # Initialize asset size
    poster_size = 0
    background_size = 0
    season_poster_size = 0
    collection_poster_size = 0
    collection_background_size = 0
    total_asset_size = 0
    completed = 0
    incomplete = 0

    try:
        library_name = library_section.title
        items = await asyncio.to_thread(library_section.all)
        total_items = len(items)
        log_processing_event("processing_library_items", library_name=library_name, total_items=total_items)

        for item in items:
            await get_plex_metadata(
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

        # Asset size aggregators by type
        async def process_and_collect(item):
            stats = await process_item(
                item, consolidated_metadata, config, dry_run, existing_yaml_data,
                library_name, existing_assets, session=session, ignored_fields=ignored_fields
            )
            if isinstance(stats, tuple):
                stats, collection_asset_paths = stats
                all_collection_asset_paths.update(collection_asset_paths)
            # Aggregate asset sizes by config
            if stats and isinstance(stats, dict):
                if config["assets"].get("run_poster", True):
                    nonlocal poster_size
                    poster_size += stats.get("poster", {}).get("size", 0)
                if config["assets"].get("run_background", False):
                    nonlocal background_size
                    background_size += stats.get("background", {}).get("size", 0)
                if config["assets"].get("run_season", True):
                    nonlocal season_poster_size
                    if "season_posters" in stats:
                        season_poster_size += sum(stats["season_posters"].values())
                    else:
                        season_poster_size += stats.get("season_poster", {}).get("size", 0)
                if config["assets"].get("run_collection", False):
                    nonlocal collection_poster_size
                    collection_poster_size += stats.get("collection_poster", {}).get("size", 0)
                    nonlocal collection_background_size
                    collection_background_size += stats.get("collection_background", {}).get("size", 0)
                # For total asset size, sum only enabled types
                nonlocal total_asset_size
                total_asset_size = (
                    poster_size + background_size + season_poster_size +
                    collection_poster_size + collection_background_size
                )
                # Completion tracking (metadata)
                if config["metadata"].get("run_basic", True):
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
                log_processing_event("processing_saving_metadata", output_path=output_path)
                with open(output_path, "w", encoding="utf-8") as f:
                    yaml = YAML()
                    yaml.default_flow_style = False
                    yaml.allow_unicode = True
                    yaml.dump(consolidated_metadata, f)
                log_processing_event("processing_metadata_saved", output_path=output_path)

                # Save cache and failed cache using always-fresh loads
                log_processing_event("processing_saving_cache")
                save_cache(load_cache())
                log_processing_event("processing_cache_saved")

            except Exception as e:
                log_processing_event("processing_failed_write_metadata", error=str(e))
        else:
            log_processing_event("processing_metadata_dry_run", library_name=library_name)

        # Prepare summary variables
        run_metadata = config["metadata"].get("run_basic", True) or config["metadata"].get("run_enhanced", False)
        percent_complete = round((completed / total_items) * 100, 2) if total_items else 0.0
        asset_summaries = []
        if config["assets"].get("run_poster", True) and poster_size > 0:
            asset_summaries.append(f"Poster: {human_readable_size(poster_size)}")
        if config["assets"].get("run_background", False) and background_size > 0:
            asset_summaries.append(f"Background: {human_readable_size(background_size)}")
        if config["assets"].get("run_season", True) and season_poster_size > 0:
            asset_summaries.append(f"Season: {human_readable_size(season_poster_size)}")
        if config["assets"].get("run_collection", False):
            if collection_poster_size > 0:
                asset_summaries.append(f"Collection Poster: {human_readable_size(collection_poster_size)}")
            if collection_background_size > 0:
                asset_summaries.append(f"Collection Background: {human_readable_size(collection_background_size)}")
        if library_filesize is not None and library_filesize[library_name] > 0:
            asset_summaries.append(f"Total: {human_readable_size(library_filesize[library_name])} downloaded")

        log_library_summary(
            library_name=library_name,
            completed=completed,
            incomplete=incomplete,
            total_items=total_items,
            percent_complete=percent_complete,
            asset_summaries=asset_summaries,
            library_filesize=library_filesize,
            run_metadata=run_metadata,
            logger=logging.getLogger()
        )

        if metadata_summaries is not None:
            metadata_summaries[library_name] = {
                "complete": completed,
                "incomplete": incomplete,
                "total_items": total_items,
                "percent_complete": percent_complete if run_metadata else None,
            }
    except Exception as e:
        log_processing_event("processing_library_failed", library_name=library_name, error=str(e))