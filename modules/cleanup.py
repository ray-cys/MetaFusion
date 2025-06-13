import logging
from pathlib import Path
from helper.config import load_config
from helper.cache import load_cache, save_cache
from helper.plex import get_plex_movie_directory, get_plex_show_directory

config = load_config()

async def cleanup_orphans(plex, libraries=None, asset_path=None, existing_assets=None):
    from ruamel.yaml import YAML
    import asyncio

    logging.info("[Cleanup] Starting orphan cleanup...")

    movie_cache = {}
    episode_cache = {}

    orphans_removed = 0
    global_valid_cache_keys = set()
    global_existing_titles = set()
    plex_sections = plex.library.sections()
    selected_libraries = libraries if libraries else [section.title for section in plex_sections]

    # Build global valid cache keys and YAML keys for all libraries
    for section in plex_sections:
        library_name = section.title
        if library_name not in selected_libraries:
            continue

        media_type = section.TYPE if hasattr(section, "TYPE") else section.type
        for item in section.all():
            title = getattr(item, "title", None)
            year = getattr(item, "year", None)
            if title and year:
                if media_type in ["show", "tv"]:
                    global_valid_cache_keys.add(f"tv:{title}:{year}")
                    if hasattr(item, "seasons"):
                        for season in item.seasons():
                            season_number = season.index
                            global_valid_cache_keys.add(f"tv:{title}:{year}:season{season_number}")
                else:
                    global_valid_cache_keys.add(f"movie:{title}:{year}")
                global_existing_titles.add(f"{title} ({year})")

    # Metadata orphan cleanup (TMDb cache)
    cache = load_cache()  # Always load fresh cache
    cache_keys_to_remove = [
        key for key in list(cache.keys())
        if key not in global_valid_cache_keys
    ]
    for key in cache_keys_to_remove:
        del cache[key]
        orphans_removed += 1
        logging.info(f"[Cleanup] Removed orphaned cache entry: {key}")
    save_cache(cache)

    # YAML metadata cleanup
    preferred_libraries = config.get("preferred_libraries", ["Movies", "TV Shows"])
    preferred_filenames = {
        f"{lib.lower().replace(' ', '_')}.yml" for lib in preferred_libraries
    }
    for metadata_file in Path(config["metadata_path"]).glob("*.yml"):
        if metadata_file.name not in preferred_filenames:
            logging.info(f"[Cleanup] Skipping non-preferred library file: {metadata_file.name}")
            continue
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                yaml = YAML()
                metadata_content = yaml.load(f) or {}

            metadata_entries = metadata_content.get("metadata", {})
            cleaned_metadata = {k: v for k, v in metadata_entries.items() if k in global_existing_titles}

            orphans_in_file = len(metadata_entries) - len(cleaned_metadata)
            orphans_removed += orphans_in_file

            # Only write if orphans were actually removed
            if orphans_in_file > 0:
                metadata_content["metadata"] = cleaned_metadata
                with open(metadata_file, "w", encoding="utf-8") as f:
                    yaml.default_flow_style = False
                    yaml.allow_unicode = True
                    yaml.dump(metadata_content, f)
                logging.info(f"[Cleanup] Removed {orphans_in_file} orphans from {metadata_file.name}")

        except Exception as e:
            logging.error(f"[Cleanup] Failed processing {metadata_file}: {e}")

    # Asset orphan cleanup (parallelized)
    if asset_path:
        valid_asset_dirs = set()
        for section in plex_sections:
            media_type = section.TYPE if hasattr(section, "TYPE") else section.type
            for item in section.all():
                if media_type in ["movie"]:
                    dir_name = await get_plex_movie_directory(item, _movie_cache=movie_cache)
                    if dir_name:
                        valid_asset_dirs.add(dir_name)
                elif media_type in ["show", "tv"]:
                    dir_name = await get_plex_show_directory(item, _episode_cache=episode_cache)
                    if dir_name:
                        valid_asset_dirs.add(dir_name)

        async def remove_orphaned_file(path, description):
            nonlocal orphans_removed
            resolved_path = str(path.resolve())
            parent_dir = path.parent.name
            if parent_dir in valid_asset_dirs:
                return
            if existing_assets is not None and resolved_path in existing_assets:
                logging.info(f"[Cleanup] Skipping in-use {description}: {path}")
                return
            action_msg = "[Dry Run] Would remove" if config.get("dry_run", False) else "Removing"
            logging.info(f"[Cleanup] {action_msg} orphaned {description}: {path}")
            if not config.get("dry_run", False):
                try:
                    await asyncio.to_thread(path.unlink)
                    orphans_removed += 1
                    # Remove empty parent directory if needed
                    if not any(path.parent.iterdir()):
                        parent_action_msg = "[Dry Run] Would remove" if config.get("dry_run", False) else "Removing"
                        logging.info(f"[Cleanup] {parent_action_msg} empty directory: {path.parent}")
                        await asyncio.to_thread(path.parent.rmdir)
                except Exception as e:
                    logging.warning(f"[Cleanup] Failed to remove orphaned {description} {path}: {e}")

        # Gather all orphaned files to remove
        orphaned_posters = [p for p in Path(asset_path).rglob("poster.jpg")]
        orphaned_season_posters = [p for p in Path(asset_path).rglob("Season*.jpg")]

        await asyncio.gather(
            *(remove_orphaned_file(p, "poster") for p in orphaned_posters),
            *(remove_orphaned_file(p, "season poster") for p in orphaned_season_posters),
        )
        logging.info(f"[Cleanup] Asset orphan cleanup complete.")

    logging.info(f"[Cleanup] Total orphans removed: {orphans_removed}")
    return orphans_removed