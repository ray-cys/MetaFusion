import json
import logging
import yaml
from pathlib import Path
from helper.tmdb import tmdb_cache, save_cache
from helper.config import load_config

config = load_config()
METADATA_DIR = Path(config["metadata_path"])

def cleanup_orphans(plex, libraries=None, asset_path=None, poster_filename=None, season_filename=None, summary=None, existing_assets=None):
    """
    Cleans up orphaned metadata entries and asset files.

    This function removes:
      - Orphaned TMDb cache entries (not present in Plex libraries)
      - Orphaned metadata entries in YAML files
      - Orphaned asset files (posters/season posters) not associated with any current Plex item

    Args:
        plex: The Plex server object.
        libraries (list, optional): List of library names to check for assets.
        asset_path (str or Path, optional): Path to the assets directory.
        poster_filename (str, optional): Filename pattern for posters.
        season_filename (str, optional): Filename pattern for season posters.
        summary (dict, optional): Dictionary to track number of removed orphans.

    Returns:
        int: Total number of orphans removed.
    """
    logging.info("[Cleanup] Starting orphan cleanup...")

    orphans_removed = 0
    cache_keys_to_remove = []

    # --- Metadata Orphan Cleanup ---
    # Build a set of all existing titles (Title (Year)) in all Plex libraries
    existing_titles = set()
    for section in plex.library.sections():
        for item in section.all():
            title = getattr(item, "title", None)
            year = getattr(item, "year", None)
            if title and year:
                existing_titles.add(f"{title} ({year})")

    # Remove TMDb cache entries not present in existing_titles
    for key in list(tmdb_cache.keys()):
        if key not in existing_titles:
            cache_keys_to_remove.append(key)

    for key in cache_keys_to_remove:
        del tmdb_cache[key]
        orphans_removed += 1
        logging.info(f"[Cleanup] Removed orphaned cache entry: {key}")

    save_cache(tmdb_cache)

    # Only process preferred libraries' metadata files
    preferred_libraries = config.get("preferred_libraries", ["Movies", "TV Shows"])
    preferred_filenames = {
        f"{lib.lower().replace(' ', '_')}.yml" for lib in preferred_libraries
    }

    # Remove orphaned metadata entries from YAML files
    for metadata_file in METADATA_DIR.glob("*.yml"):
        if metadata_file.name not in preferred_filenames:
            logging.info(f"[Cleanup] Skipping non-preferred library file: {metadata_file.name}")
            continue
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata_content = yaml.safe_load(f) or {}

            metadata_entries = metadata_content.get("metadata", {})
            cleaned_metadata = {k: v for k, v in metadata_entries.items() if k in existing_titles}

            orphans_in_file = len(metadata_entries) - len(cleaned_metadata)
            orphans_removed += orphans_in_file

            if orphans_in_file > 0:
                metadata_content["metadata"] = cleaned_metadata
                with open(metadata_file, "w", encoding="utf-8") as f:
                    yaml.dump(metadata_content, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
                logging.info(f"[Cleanup] Removed {orphans_in_file} orphans from {metadata_file.name}")

        except Exception as e:
            logging.error(f"[Cleanup] Failed processing {metadata_file}: {e}")

    # --- Asset Orphan Cleanup ---
    # Only run if all asset parameters are provided
    if libraries and asset_path and poster_filename and season_filename:
        valid_keys = set()
        for lib in libraries:
            section = plex.library.section(lib)
            items = section.all()
            for item in items:
                title = getattr(item, "title", None)
                year = getattr(item, "year", None)
                media_type = "movie" if getattr(item, "TYPE", None) == "movie" else "tv"
                cache_key = f"{media_type}:{title}:{year}"
                valid_keys.add(cache_key)
                # For TV shows, add season cache keys
                if media_type == "tv":
                    for season in item.seasons():
                        season_number = season.index
                        season_cache_key = f"tv:{title}:{year}:season{season_number}"
                        valid_keys.add(season_cache_key)

        # Remove invalid cache keys (not in valid_keys)
        invalid_keys = set(tmdb_cache.keys()) - valid_keys
        for key in invalid_keys:
            action_msg = "[Dry Run] Would invalidate" if config.get("dry_run", False) else "Invalidating"
            logging.info(f"[Library Cleanup] {action_msg} cache entry: {key}")
            if not config.get("dry_run", False):
                del tmdb_cache[key]

        def remove_orphaned_files(pattern, description):
            """
            Remove orphaned asset files matching a pattern.

            Args:
                pattern (str): Glob pattern for files.
                description (str): Description for logging.
            """
            for path in Path(asset_path).rglob(pattern):
                resolved_path = str(path.resolve())
                # Only remove if not in existing_assets
                if existing_assets is not None and resolved_path in existing_assets:
                    logging.info(f"[Library Cleanup] Skipping in-use {description}: {path}")
                    continue
                action_msg = "[Dry Run] Would remove" if config.get("dry_run", False) else "Removing"
                logging.info(f"[Library Cleanup] {action_msg} orphaned {description}: {path}")
                if not config.get("dry_run", False):
                    try:
                        parent_dir = path.parent
                        path.unlink()
                        if summary is not None:
                            summary["removed"] = summary.get("removed", 0) + 1
                        # Remove empty parent dir if needed
                        if not any(parent_dir.iterdir()):
                            parent_action_msg = "[Dry Run] Would remove" if config.get("dry_run", False) else "Removing"
                            logging.info(f"[Library Cleanup] {parent_action_msg} empty directory: {parent_dir}")
                            parent_dir.rmdir()
                    except Exception as e:
                        logging.warning(f"[Library Cleanup] Failed to remove orphaned {description} {path}: {e}")

        # Remove orphaned poster and season poster files
        remove_orphaned_files(poster_filename, "poster")
        remove_orphaned_files(season_filename.replace("{season_number:02}", "*"), "season poster")

        # Save cache if any invalid keys were removed
        if invalid_keys and not config.get("dry_run", False):
            save_cache(tmdb_cache)
            logging.info(f"[Library Cleanup] TMDb cache saved after cleanup.")
        elif config.get("dry_run", False):
            logging.info("[Dry Run] Would save updated TMDb cache after cleanup.")
        else:
            logging.info("[Library Cleanup] TMDb cache unchanged; no need to save.")

    logging.info(f"[Cleanup] Total orphans removed: {orphans_removed}")
    return orphans_removed