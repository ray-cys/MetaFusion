import logging
from pathlib import Path
from helper.config import load_config
from helper.tmdb import tmdb_cache, save_cache
from helper.plex import get_plex_movie_directory, get_plex_show_directory

config = load_config()

def cleanup_orphans(plex, libraries=None, asset_path=None, existing_assets=None,):
    from ruamel.yaml import YAML
    """
    Cleans up orphaned metadata entries and asset files.
    """
    logging.info("[Cleanup] Starting orphan cleanup...")

    orphans_removed = 0
    global_valid_cache_keys = set()
    global_existing_titles = set()
    plex_sections = plex.library.sections()
    selected_libraries = libraries if libraries else [section.title for section in plex_sections]

    # --- Build global valid cache keys and YAML keys for all libraries ---
    for section in plex_sections:
        library_name = section.title
        if library_name not in selected_libraries:
            continue

        media_type = section.TYPE if hasattr(section, "TYPE") else section.type
        for item in section.all():
            title = getattr(item, "title", None)
            year = getattr(item, "year", None)
            if title and year:
                # Add all possible prefixes for TV
                if media_type in ["show", "tv"]:
                    global_valid_cache_keys.add(f"tv:{title}:{year}")
                    if hasattr(item, "seasons"):
                        for season in item.seasons():
                            season_number = season.index
                            global_valid_cache_keys.add(f"tv:{title}:{year}:season{season_number}")
                else:
                    global_valid_cache_keys.add(f"movie:{title}:{year}")
                global_existing_titles.add(f"{title} ({year})")

    # --- Metadata Orphan Cleanup (TMDb cache) ---
    cache_keys_to_remove = [key for key in list(tmdb_cache.keys()) if key not in global_valid_cache_keys]
    for key in cache_keys_to_remove:
        del tmdb_cache[key]
        orphans_removed += 1
        logging.info(f"[Cleanup] Removed orphaned cache entry: {key}")
    save_cache(tmdb_cache)

    # --- YAML Metadata Cleanup ---
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

            if orphans_in_file > 0:
                metadata_content["metadata"] = cleaned_metadata
                with open(metadata_file, "w", encoding="utf-8") as f:
                    yaml.default_flow_style = False
                    yaml.allow_unicode = True
                    yaml.dump(metadata_content, f)
                logging.info(f"[Cleanup] Removed {orphans_in_file} orphans from {metadata_file.name}")

        except Exception as e:
            logging.error(f"[Cleanup] Failed processing {metadata_file}: {e}")

    # --- Asset Orphan Cleanup ---
    if asset_path:
        # Build set of valid asset directories from Plex
        valid_asset_dirs = set()
        for section in plex_sections:
            media_type = section.TYPE if hasattr(section, "TYPE") else section.type
            for item in section.all():
                if media_type in ["movie"]:
                    dir_name = get_plex_movie_directory(item)
                    if dir_name:
                        valid_asset_dirs.add(dir_name)
                elif media_type in ["show", "tv"]:
                    dir_name = get_plex_show_directory(item)
                    if dir_name:
                        valid_asset_dirs.add(dir_name)

        def remove_orphaned_files(pattern, description):
            """
            Remove orphaned asset files matching a pattern.
            """
            nonlocal orphans_removed
            for path in Path(asset_path).rglob(pattern):
                resolved_path = str(path.resolve())
                # Check if this asset's parent directory matches a valid Plex directory
                parent_dir = path.parent.name
                if parent_dir in valid_asset_dirs:
                    continue  # This asset is still in use by Plex
                if existing_assets is not None and resolved_path in existing_assets:
                    logging.info(f"[Library Cleanup] Skipping in-use {description}: {path}")
                    continue
                action_msg = "[Dry Run] Would remove" if config.get("dry_run", False) else "Removing"
                logging.info(f"[Library Cleanup] {action_msg} orphaned {description}: {path}")
                if not config.get("dry_run", False):
                    try:
                        path.unlink()
                        orphans_removed += 1
                        # Remove empty parent directory if needed
                        if not any(path.parent.iterdir()):
                            parent_action_msg = "[Dry Run] Would remove" if config.get("dry_run", False) else "Removing"
                            logging.info(f"[Library Cleanup] {parent_action_msg} empty directory: {path.parent}")
                            path.parent.rmdir()
                    except Exception as e:
                        logging.warning(f"[Library Cleanup] Failed to remove orphaned {description} {path}: {e}")

        # Remove orphaned poster and season poster files
        remove_orphaned_files("poster.jpg", "poster")
        remove_orphaned_files("Season*.jpg", "season poster")
        logging.info(f"[Library Cleanup] Asset orphan cleanup complete.")

    logging.info(f"[Cleanup] Total orphans removed: {orphans_removed}")
    return orphans_removed