import asyncio
from ruamel.yaml import YAML
from pathlib import Path
from helper.logging import log_cleanup_event
from helper.cache import load_cache, save_cache
from helper.plex import get_plex_metadata, connect_plex_library

async def cleanup_orphans(
    logger,
    config,
    libraries=None, 
    asset_path=None, 
    existing_assets=None, 
    valid_collection_assets=None,
):
    log_cleanup_event("cleanup_start")
    movie_cache = {}
    episode_cache = {}

    orphans_removed = 0
    global_valid_cache_keys = set()
    global_existing_titles = set()

    plex, sections, libraries_info, selected_libraries = connect_plex_library(config, logger, libraries)
    for section in sections:
        library_name = section.title
        media_type = section.TYPE if hasattr(section, "TYPE") else section.type
        for item in section.all():
            meta = await get_plex_metadata(item, _movie_cache=movie_cache, _episode_cache=episode_cache)
            title = meta.get("title")
            year = meta.get("year")
            if title and year:
                if media_type in ["show", "tv"]:
                    global_valid_cache_keys.add(f"tv:{title}:{year}")
                    seasons_episodes = meta.get("seasons_episodes") or {}
                    for season_number in seasons_episodes:
                        global_valid_cache_keys.add(f"tv:{title}:{year}:season{season_number}")
                else:
                    global_valid_cache_keys.add(f"movie:{title}:{year}")
                global_existing_titles.add(f"{title} ({year})")

    cache = load_cache()
    cache_keys_to_remove = [
        key for key in list(cache.keys())
        if key not in global_valid_cache_keys
    ]
    for key in cache_keys_to_remove:
        del cache[key]
        orphans_removed += 1
        log_cleanup_event("cleanup_removed_cache_entry", key=key)
    save_cache(cache)

    plex_libraries = config.get("plex_libraries", ["Movies", "TV Shows"])
    preferred_filenames = {
        f"{lib.lower().replace(' ', '_')}.yml" for lib in plex_libraries
    }
    for metadata_file in Path(config["metadata"]["directory"]).glob("*.yml"):
        if metadata_file.name not in preferred_filenames:
            log_cleanup_event("cleanup_skipping_nonpreferred", filename=metadata_file.name)
            continue
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                yaml = YAML()
                metadata_content = yaml.load(f) or {}

            metadata_entries = metadata_content.get("metadata", {})
            cleaned_metadata = {k: v for k, v in metadata_entries.items() if k in global_existing_titles}

            orphans_in_file = len(metadata_entries) - len(cleaned_metadata)
            orphans_removed += orphans_in_file

            if orphans_in_file > 0:
                metadata_content["metadata"] = cleaned_metadata
                with open(metadata_file, "w", encoding="utf-8") as f:
                    yaml.default_flow_style = False
                    yaml.allow_unicode = True
                    yaml.dump(metadata_content, f)
                log_cleanup_event("cleanup_removed_orphans", orphans_in_file=orphans_in_file, filename=metadata_file.name)

        except Exception as e:
            log_cleanup_event("cleanup_failed_remove_metadata", filename=metadata_file, error=str(e))

    if asset_path:
        valid_asset_dirs = set()
        for section in sections:
            media_type = section.TYPE if hasattr(section, "TYPE") else section.type
            for item in section.all():
                meta = await get_plex_metadata(item, _movie_cache=movie_cache, _episode_cache=episode_cache)
                if media_type == "movie":
                    dir_name = meta.get("movie_path")
                    if dir_name:
                        valid_asset_dirs.add(dir_name)
                elif media_type in ["show", "tv"]:
                    dir_name = meta.get("show_path")
                    if dir_name:
                        valid_asset_dirs.add(dir_name)

        async def remove_orphaned_file(path, description):
            nonlocal orphans_removed
            resolved_path = str(path.resolve())
            parent_dir = path.parent.name
            if valid_collection_assets and resolved_path in valid_collection_assets:
                log_cleanup_event("cleanup_skipping_collection_asset", description=description, path=path)
                return
            if parent_dir in valid_asset_dirs:
                return
            if existing_assets is not None and resolved_path in existing_assets:
                log_cleanup_event("cleanup_skipping_valid_asset", description=description, path=path)
                return
            action_msg = "[Dry Run] Would remove" if config["settings"].get("dry_run", False) else "Removing"
            log_cleanup_event("cleanup_removing_asset", action_msg=action_msg, description=description, path=path)
            if not config["settings"].get("dry_run", False):
                try:
                    await asyncio.to_thread(path.unlink)
                    orphans_removed += 1
                    if not any(path.parent.iterdir()):
                        parent_action_msg = "[Dry Run] Would remove" if config["settings"].get("dry_run", False) else "Removing"
                        log_cleanup_event("cleanup_removing_empty_dir", parent_action_msg=parent_action_msg, parent=path.parent)
                        await asyncio.to_thread(path.parent.rmdir)
                except Exception as e:
                    log_cleanup_event("cleanup_failed_remove_asset", description=description, path=path, error=str(e))

        orphaned_posters = [p for p in Path(asset_path).rglob("poster.jpg")]
        orphaned_season_posters = [p for p in Path(asset_path).rglob("Season*.jpg")]
        orphaned_backgrounds = [p for p in Path(asset_path).rglob("fanart.jpg")]
            
        await asyncio.gather(
            *(remove_orphaned_file(p, "poster") for p in orphaned_posters),
            *(remove_orphaned_file(p, "season poster") for p in orphaned_season_posters),
            *(remove_orphaned_file(p, "background") for p in orphaned_backgrounds),
        )

    log_cleanup_event("cleanup_total_removed", orphans_removed=orphans_removed)
    return orphans_removed