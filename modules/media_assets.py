import logging
import shutil
from pathlib import Path
from helper.config import load_config
from helper.tmdb import tmdb_cache, cache_lock, save_cache, update_tmdb_cache
from helper.stats import human_readable_size
from modules.assets import (
    download_poster, should_upgrade, generate_temp_path, get_best_poster
)
from helper.plex import get_plex_movie_directory, get_plex_show_directory, safe_title_year
from helper.tmdb import safe_get_with_retries

config = load_config()

def process_poster_for_media(media_type, tmdb_id, item, library_name, existing_assets):
    """
    Download and process the best poster for a movie or TV show item.
    """
    logging.debug(f"[Script State] Processing Movies & TV Shows poster: TMDb ID {tmdb_id}")
    preferred_language = config["tmdb"].get("language", "en").split("-")[0]
    # Fetch metadata and images from TMDb
    response = safe_get_with_retries(
        f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}",
        params={
            "api_key": config["tmdb"]["api_key"],
            "append_to_response": "images",
            "language": config["tmdb"].get("language", "en"),
            "include_image_language": f"{preferred_language},null"
        }
    )

    if not response:
        logging.warning(f"[Assets Fetch] Failed to fetch poster data for {safe_title_year(item)}. Skipping...")
        return 0

    response_data = response.json()
    images = response_data.get("images", {}).get("posters", [])
    fallback = config["tmdb"].get("fallback", [])
    best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)

    if not best:
        logging.info(f"[Assets Fetch] No suitable poster found for {safe_title_year(item)}. Skipping...")
        return 0

    # Determine asset path based on media type
    parent_dir = get_plex_show_directory(item) if media_type == "tv" else get_plex_movie_directory(item)
    asset_path = Path(config["assets_path"]) / library_name / parent_dir / "poster.jpg"
    temp_path = generate_temp_path(library_name)
    # Standardize media_type
    title = getattr(item, "title", "Unknown")
    year = getattr(item, "year", "Unknown")
    media_type = media_type.lower() if "media_type" in locals() else "tv"
    if media_type == "show":
        media_type = "tv"
    elif media_type != "movie" and media_type != "tv":
        media_type = "movie"
    cache_key = f"{media_type}:{title}:{year}"
    
    downloaded_size = 0
    try:
        if config.get("dry_run", False):
            logging.info(f"[Assets Dry Run] Would process poster for {safe_title_year(item)}")
            existing_assets.add(str(asset_path.resolve()))
            return 0

        # Download poster and check if upgrade is needed
        if download_poster(best["file_path"], temp_path, item) and temp_path.exists():
            downloaded_size = temp_path.stat().st_size
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Assets Upgrade] Poster upgraded for {safe_title_year(item)}. Filesize: {human_readable_size(downloaded_size)}")
                # Update cache with new vote_average
                with cache_lock:
                    if cache_key in tmdb_cache and isinstance(tmdb_cache[cache_key], dict):
                        update_tmdb_cache(cache_key, tmdb_id, title, year, media_type, vote_average=best.get("vote_average"))
                    else:
                        tmdb_cache[cache_key] = {
                            "tmdb_id": tmdb_id,
                            "vote_average": best.get("vote_average")
                        }
                    save_cache(tmdb_cache)
            else:
                temp_path.unlink(missing_ok=True)
                logging.info(f"[Assets Upgrade] No upgrade needed for {safe_title_year(item)}")
        else:
            logging.warning(f"[Assets Upgrade] Poster download failed for {safe_title_year(item)}, skipping...")
    finally:
        # Clean up temp file if it still exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    # Track processed asset
    existing_assets.add(str(asset_path.resolve()))
    return downloaded_size

def process_season_poster(tmdb_id, season_number, item, library_name, existing_assets):
    """
    Download and process the best poster for a specific TV season.
    """
    logging.debug(f"[Script State] Processing Season poster: TMDb ID {tmdb_id}, Season {season_number}")
    preferred_language = config["tmdb"].get("language", "en").split("-")[0]
    # Fetch season metadata and images from TMDb
    response = safe_get_with_retries(
        f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season_number}",
        params={
            "api_key": config["tmdb"]["api_key"],
            "append_to_response": "images",
            "language": config["tmdb"].get("language", "en"),
            "include_image_language": f"{preferred_language},null"
        }
    )

    if not response or response.status_code == 404:
        logging.info(f"[Assets Fetch] No posters found or season not found for {safe_title_year(item)} Season {season_number}. Skipping...")
        return 0

    response_data = response.json()
    images = response_data.get("images", {}).get("posters", [])
    if not images:
        logging.info(f"[Assets Fetch] No season posters available for {safe_title_year(item)} Season {season_number}.")
        return 0

    fallback = config["tmdb"].get("fallback", [])
    best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)
    if not best:
        logging.info(f"[Assets Fetch] No suitable season poster found for {safe_title_year(item)} Season {season_number}. Skipping...")
        return 0

    parent_dir = get_plex_show_directory(item)
    asset_path = Path(config["assets_path"]) / library_name / parent_dir / f"Season{season_number:02}.jpg"
    temp_path = generate_temp_path(library_name)
    title = getattr(item, "title", "Unknown")
    year = getattr(item, "year", "Unknown")
    media_type = "tv"
    cache_key = f"tv:{title}:{year}:season{season_number}"
    
    downloaded_size = 0
    try:
        if config.get("dry_run", False):
            logging.info(f"[Assets Dry Run] Would process season poster for {safe_title_year(item)} Season {season_number}")
            existing_assets.add(str(asset_path.resolve()))
            return 0

        # Download poster and check if upgrade is needed
        if download_poster(best["file_path"], temp_path, item) and temp_path.exists():
            downloaded_size = temp_path.stat().st_size
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item, season_number=season_number):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Assets Upgrade] Season poster upgraded for {safe_title_year(item)} Season {season_number}. Filesize: {human_readable_size(downloaded_size)}")
                with cache_lock:
                    if cache_key in tmdb_cache and isinstance(tmdb_cache[cache_key], dict):
                        update_tmdb_cache(cache_key, tmdb_id, title, year, media_type, vote_average=best.get("vote_average"))
                    else:
                        tmdb_cache[cache_key] = {
                            "tmdb_id": tmdb_id,
                            "vote_average": best.get("vote_average")
                        }
                    save_cache(tmdb_cache)
            else:
                temp_path.unlink(missing_ok=True)
                logging.info(f"[Assets Upgrade] No season poster upgrade needed for {safe_title_year(item)} Season {season_number}")
        else:
            logging.warning(f"[Assets Upgrade] Failed to download season poster for {safe_title_year(item)} Season {season_number}. Skipping...")
    finally:
        # Clean up temp file if it still exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    # Track processed asset
    existing_assets.add(str(asset_path.resolve()))
    return downloaded_size