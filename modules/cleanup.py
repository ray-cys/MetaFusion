import asyncio
from ruamel.yaml import YAML
from pathlib import Path
from helper.logging import log_cleanup_event
from helper.cache import load_cache, save_cache

async def cleanup_title_orphans(
    config, libraries=None, asset_path=None, existing_assets=None, 
    preloaded_plex_metadata=None, valid_collection_assets=None,
):
    log_cleanup_event("cleanup_start")
    orphans_removed = 0
    global_valid_cache_keys = set()
    global_existing_titles = set()
    removed_summary = {}

    # Validate Plex metadata
    if preloaded_plex_metadata is None:
        log_cleanup_event("cleanup_error")
        return orphans_removed

    # Build valid keys and titles from metadata
    for (title, year, media_type), meta in preloaded_plex_metadata.items():
        if title and year:
            if media_type in ["show", "tv"]:
                global_valid_cache_keys.add(f"tv:{title}:{year}")
                seasons_episodes = meta.get("seasons_episodes") or {}
                for season_number in seasons_episodes:
                    global_valid_cache_keys.add(f"tv:{title}:{year}:season{season_number}")
            else:
                global_valid_cache_keys.add(f"movie:{title}:{year}")
            global_existing_titles.add(f"{title} ({year})")

    # Cache cleanup
    cache = load_cache()
    cache_keys_to_remove = [
        key for key in list(cache.keys())
        if key not in global_valid_cache_keys
    ]
    for key in cache_keys_to_remove:
        # Try to extract title/year for summary
        title, year = None, None
        if key.startswith("movie:") or key.startswith("tv:"):
            try:
                _, rest = key.split(":", 1)
                title, year = rest.rsplit(":", 1) if ":" in rest else rest.rsplit(",", 1)
                title = title.strip()
                year = year.strip()
            except Exception:
                pass
        if title and year:
            removed_summary.setdefault((title, year), {"cache": False, "asset": [], "yaml": False})
            removed_summary[(title, year)]["cache"] = True
        if config.get("settings", {}).get("dry_run", False):
            log_cleanup_event("cleanup_dry_run", description="cache", path=key)
        else:
            log_cleanup_event("cleanup_removed_cache_entry", key=key)
            del cache[key]
            orphans_removed += 1
    save_cache(cache)

    # YAML metadata cleanup
    plex_libraries = config.get("plex_libraries", [])
    preferred_filenames = {
        f"{lib.lower().replace(' ', '_')}.yml" for lib in plex_libraries
    }
    metadata_dir = Path(config["metadata"]["directory"])
    def extract_title_year(orphan_title):
        if " (" in orphan_title and orphan_title.endswith(")"):
            t, y = orphan_title.rsplit(" (", 1)
            y = y.rstrip(")")
        else:
            t, y = orphan_title, None
        return t, y

    for metadata_file in metadata_dir.glob("*.yml"):
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

            if orphans_in_file > 0:
                for orphan_title in set(metadata_entries) - set(cleaned_metadata):
                    t, y = extract_title_year(orphan_title)
                    if t and y:
                        removed_summary.setdefault((t, y), {"cache": False, "asset": [], "yaml": False})
                        removed_summary[(t, y)]["yaml"] = True
                if config.get("settings", {}).get("dry_run", False):
                    log_cleanup_event("cleanup_dry_run", description=cleaned_metadata, path=metadata_file)
                else:
                    metadata_content["metadata"] = cleaned_metadata
                    with open(metadata_file, "w", encoding="utf-8") as f:
                        yaml.default_flow_style = False
                        yaml.allow_unicode = True
                        yaml.dump(metadata_content, f)
                    log_cleanup_event("cleanup_removed_orphans", orphans_in_file=orphans_in_file, filename=metadata_file.name)
                orphans_removed += orphans_in_file

        except Exception as e:
            log_cleanup_event("cleanup_failed_remove_metadata", filename=metadata_file, error=str(e))

    # Asset cleanup controlled by config["assets"] switches
    assets_config = config.get("assets", {})
    run_poster = assets_config.get("run_poster", True)
    run_season = assets_config.get("run_season", True)
    run_background = assets_config.get("run_background", True)

    if asset_path:
        valid_asset_dirs = set()
        for (title, year, media_type), meta in preloaded_plex_metadata.items():
            if media_type == "movie":
                dir_name = meta.get("movie_path")
                if dir_name:
                    valid_asset_dirs.add(dir_name)
            elif media_type in ["show", "tv"]:
                dir_name = meta.get("show_path")
                if dir_name:
                    valid_asset_dirs.add(dir_name)

        async def remove_orphaned_file(path, description, strict):
            nonlocal orphans_removed
            # Try to extract title/year for summary
            title, year = None, None
            try:
                parent = path.parent
                if " (" in parent.name and parent.name.endswith(")"):
                    title, year = parent.name.rsplit(" (", 1)
                    year = year.rstrip(")")
            except Exception:
                pass
            if title and year:
                removed_summary.setdefault((title, year), {"cache": False, "asset": [], "yaml": False})
                removed_summary[(title, year)]["asset"].append(description)
            resolved_path = str(path.resolve())
            parent_dir = path.parent.name
            # If strict, only remove if not in Plex metadata
            if strict:
                if parent_dir in valid_asset_dirs:
                    return
            # If not strict, always remove unless it's a collection asset or in existing_assets
            if valid_collection_assets and resolved_path in valid_collection_assets:
                log_cleanup_event("cleanup_skipping_collection_asset", description=description, path=path)
                return
            if existing_assets is not None and resolved_path in existing_assets:
                log_cleanup_event("cleanup_skipping_valid_asset", description=description, path=path)
                return
            if config.get("settings", {}).get("dry_run", False):
                log_cleanup_event("cleanup_dry_run", description=description, path=path)
            else:
                log_cleanup_event("cleanup_removing_asset", description=description, path=path)
                try:
                    await asyncio.to_thread(path.unlink)
                    orphans_removed += 1
                    # Directory removal
                    if not any(path.parent.iterdir()):
                        if config.get("settings", {}).get("dry_run", False):
                            log_cleanup_event("cleanup_dry_run", description="directory", path=path.parent)
                        else:
                            log_cleanup_event("cleanup_removing_empty_dir", parent=path.parent)
                            await asyncio.to_thread(path.parent.rmdir)
                except Exception as e:
                    log_cleanup_event("cleanup_failed_remove_asset", description=description, path=path, error=str(e))

        # Posters
        orphaned_posters = [p for p in Path(asset_path).rglob("poster.jpg")]
        if run_poster:
            await asyncio.gather(*(remove_orphaned_file(p, "poster", True) for p in orphaned_posters))
        else:
            await asyncio.gather(*(remove_orphaned_file(p, "poster", False) for p in orphaned_posters))

        # Season Posters
        orphaned_season_posters = [p for p in Path(asset_path).rglob("Season*.jpg")]
        if run_season:
            await asyncio.gather(*(remove_orphaned_file(p, "season poster", True) for p in orphaned_season_posters))
        else:
            await asyncio.gather(*(remove_orphaned_file(p, "season poster", False) for p in orphaned_season_posters))

        # Backgrounds
        orphaned_backgrounds = [p for p in Path(asset_path).rglob("fanart.jpg")]
        if run_background:
            await asyncio.gather(*(remove_orphaned_file(p, "background", True) for p in orphaned_backgrounds))
        else:
            await asyncio.gather(*(remove_orphaned_file(p, "background", False) for p in orphaned_backgrounds))

    # Log consolidated summary
    if removed_summary:
        log_cleanup_event("cleanup_consolidated_removed", removed_summary=removed_summary)

    log_cleanup_event("cleanup_total_removed", orphans_removed=orphans_removed)
    return