import asyncio, yaml
from pathlib import Path
from helper.logging import log_cleanup_event
from helper.cache import load_cache, save_cache

async def cleanup_title_orphans(
    config, feature_flags, asset_path=None, existing_assets=None, preloaded_plex_metadata=None
):
    log_cleanup_event("cleanup_start")
    orphans_removed = 0
    global_valid_cache_keys = set()
    global_existing_titles = set()
    removed_summary = {}

    if preloaded_plex_metadata is None:
        log_cleanup_event("cleanup_error")
        return orphans_removed

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

    cache = load_cache()
    cache_keys_to_remove = [
        key for key in list(cache.keys())
        if key not in global_valid_cache_keys
    ]
    for key in cache_keys_to_remove:
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
        if feature_flags.get("dry_run", False):
            log_cleanup_event("cleanup_dry_run", description="cache", path=key)
        else:
            log_cleanup_event("cleanup_removed_cache_entry", key=key)
            del cache[key]
            orphans_removed += 1
    save_cache(cache)

    library_types = {"movie", "tv", "show"} 
    preferred_filenames = {f"{lt}_metadata.yml" for lt in library_types}
    metadata_dir = Path(config["metadata"]["path"])
    def extract_title_year(orphan_title):
        if " (" in orphan_title and orphan_title.endswith(")"):
            t, y = orphan_title.rsplit(" (", 1)
            y = y.rstrip(")")
        else:
            t, y = orphan_title, None
        return t, y

    run_metadata_basic = feature_flags.get("metadata_basic", True)
    run_metadata_enhanced = feature_flags.get("metadata_enhanced", True)
    run_poster = feature_flags.get("poster", True)
    run_season = feature_flags.get("season", True)
    run_background = feature_flags.get("background", True)

    if run_metadata_basic or run_metadata_enhanced:
        for metadata_file in metadata_dir.glob("*.yml"):
            if metadata_file.name not in preferred_filenames:
                log_cleanup_event("cleanup_skipping_nonpreferred", filename=metadata_file.name)
                continue
            try:
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata_content = yaml.safe_load(f) or {}

                metadata_entries = metadata_content.get("metadata", {})
                cleaned_metadata = {k: v for k, v in metadata_entries.items() if k in global_existing_titles}

                orphans_in_file = len(metadata_entries) - len(cleaned_metadata)

                if orphans_in_file > 0:
                    for orphan_title in set(metadata_entries) - set(cleaned_metadata):
                        t, y = extract_title_year(orphan_title)
                        if t and y:
                            removed_summary.setdefault((t, y), {"cache": False, "asset": [], "yaml": False})
                            removed_summary[(t, y)]["yaml"] = True
                    if feature_flags.get("dry_run", False):
                        log_cleanup_event("cleanup_dry_run", description=cleaned_metadata, path=metadata_file)
                    else:
                        metadata_content["metadata"] = cleaned_metadata
                        with open(metadata_file, "w", encoding="utf-8") as f:
                            yaml.dump(metadata_content, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
                        log_cleanup_event("cleanup_removed_orphans", orphans_in_file=orphans_in_file, filename=metadata_file.name)
                    orphans_removed += orphans_in_file

            except Exception as e:
                log_cleanup_event("cleanup_failed_remove_metadata", filename=metadata_file, error=str(e))

    assets_config = config.get("assets", {})
    if asset_path:
        valid_asset_dirs = set()
        for (title, year, media_type), meta in preloaded_plex_metadata.items():
            if media_type == "movie":
                dir_name = meta.get("movie_path")
                if dir_name:
                    valid_asset_dirs.add(Path(dir_name).name)
            elif media_type in ["show", "tv"]:
                dir_name = meta.get("show_path")
                if dir_name:
                    valid_asset_dirs.add(Path(dir_name).name)

        deleted_dirs = set()
        async def remove_asset_title(path, description, strict):
            nonlocal orphans_removed
            title, year = None, None
            try:
                parent = path.parent
                if " (" in parent.name and parent.name.endswith(")"):
                    title, year = parent.name.rsplit(" (", 1)
                    year = year.rstrip(")")
            except Exception:
                pass
            resolved_path = str(path.resolve())
            parent_dir = path.parent.name
            if strict:
                if path.parent.name in valid_asset_dirs:
                    return
            if existing_assets is not None and resolved_path in existing_assets:
                log_cleanup_event("cleanup_skipping_valid_asset", description=description, path=path)
                return
            if feature_flags.get("dry_run", False):
                log_cleanup_event("cleanup_dry_run", description=description, path=path)
            else:
                log_cleanup_event("cleanup_removing_asset", description=description, path=path)
                try:
                    if path.exists():
                        await asyncio.to_thread(path.unlink)
                        orphans_removed += 1
                        deleted_dirs.add(str(path.parent.resolve()))
                        if title and year:
                            removed_summary.setdefault((title, year), {"cache": False, "asset": [], "yaml": False})
                            removed_summary[(title, year)]["asset"].append(description)
                    else:
                        log_cleanup_event("cleanup_failed_remove_asset", description=description, path=path, error="File does not exist")
                except Exception as e:
                    log_cleanup_event("cleanup_failed_remove_asset", description=description, path=path, error=str(e))

        orphaned_posters = [p for p in Path(asset_path).rglob("poster.jpg")]
        orphaned_season_posters = [p for p in Path(asset_path).rglob("Season*.jpg")]
        orphaned_backgrounds = [p for p in Path(asset_path).rglob("fanart.jpg")]

        tasks = []
        if run_poster:
            tasks.extend(remove_asset_title(p, "poster", True) for p in orphaned_posters)
        else:
            tasks.extend(remove_asset_title(p, "poster", False) for p in orphaned_posters)
        if run_season:
            tasks.extend(remove_asset_title(p, "season poster", True) for p in orphaned_season_posters)
        else:
            tasks.extend(remove_asset_title(p, "season poster", False) for p in orphaned_season_posters)
        if run_background:
            tasks.extend(remove_asset_title(p, "background", True) for p in orphaned_backgrounds)
        else:
            tasks.extend(remove_asset_title(p, "background", False) for p in orphaned_backgrounds)
        await asyncio.gather(*tasks)

        for dir_path_str in deleted_dirs:
            dir_path = Path(dir_path_str)
            try:
                if dir_path.exists() and dir_path.is_dir() and not any(dir_path.iterdir()):
                    if feature_flags.get("dry_run", False):
                        log_cleanup_event("cleanup_dry_run", description="directory", path=dir_path)
                    else:
                        log_cleanup_event("cleanup_removing_empty_dir", parent=dir_path)
                        await asyncio.to_thread(dir_path.rmdir)
            except Exception as e:
                log_cleanup_event("cleanup_failed_remove_asset", description="directory", path=dir_path, error=str(e))

    if removed_summary:
        log_cleanup_event("cleanup_consolidated_removed", removed_summary=removed_summary)

    unique_titles_removed = set(t for t, v in removed_summary.items() if any(v.values()))
    log_cleanup_event("cleanup_total_removed", orphans_removed=len(unique_titles_removed))
    return orphans_removed