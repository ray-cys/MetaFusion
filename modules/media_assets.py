import logging
import shutil
from pathlib import Path

from helper.config import load_config
from helper.tmdb import tmdb_cache, cache_lock, save_cache
from modules.assets import (
    download_poster, should_upgrade, generate_temp_path, get_best_poster
)
from helper.plex import get_plex_movie_directory, get_plex_show_directory, safe_title_year
from helper.tmdb import safe_get_with_retries

config = load_config()

def process_poster_for_media(media_type, tmdb_id, item, library_name, existing_assets, summary):
    """
    Download and process the best poster for a movie or TV show item.

    Fetches poster data from TMDb, selects the best poster, downloads it,
    and upgrades the asset if needed. Updates summary counters and asset tracking.

    Args:
        media_type (str): "movie" or "tv".
        tmdb_id (int): The TMDb ID for the media.
        item: The Plex item object.
        library_name (str): The name of the Plex library.
        existing_assets (set): Set of asset paths already processed.
        summary (dict): Dictionary for tracking summary statistics.

    Returns:
        None
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
        logging.warning(f"[Media Fetch] Failed to fetch poster data for {safe_title_year(item)}. Skipping...")
        return

    response_data = response.json()
    images = response_data.get("images", {}).get("posters", [])
    fallback_languages = config["tmdb"].get("fallback_languages", [])
    best = get_best_poster(images, preferred_language=preferred_language, fallback_languages=fallback_languages)

    if not best:
        logging.info(f"[Media Fetch] No suitable poster found for {safe_title_year(item)}. Skipping...")
        return

    # Determine asset path based on media type
    parent_dir = get_plex_show_directory(item) if media_type == "tv" else get_plex_movie_directory(item)
    asset_path = Path(config["assets"]["assets_path"]) / library_name / parent_dir / config["assets"].get("poster_filename", "poster.jpg")
    temp_path = generate_temp_path(library_name)
    cache_key = f"{media_type}:{item.title}:{item.year}"

    try:
        if config.get("dry_run", False):
            logging.info(f"[Dry Run] Would process poster for {safe_title_year(item)}")
            summary["skipped"] = summary.get("skipped", 0) + 1
            existing_assets.add(str(asset_path.resolve()))
            return

        # Download poster and check if upgrade is needed
        if download_poster(best["file_path"], temp_path, item) and temp_path.exists():
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Media Upgrade] Poster upgraded for {safe_title_year(item)}")
                summary["updated"] = summary.get("updated", 0) + 1
                # Update cache with new vote_average
                with cache_lock:
                    if cache_key in tmdb_cache and isinstance(tmdb_cache[cache_key], dict):
                        tmdb_cache[cache_key]["vote_average"] = best.get("vote_average")
                    else:
                        tmdb_cache[cache_key] = {
                            "tmdb_id": tmdb_id,
                            "vote_average": best.get("vote_average")
                        }
                    save_cache(tmdb_cache)
            else:
                temp_path.unlink(missing_ok=True)
                logging.info(f"[Media Upgrade] No upgrade needed for {safe_title_year(item)}")
                summary["skipped"] = summary.get("skipped", 0) + 1
        else:
            logging.warning(f"[Media Upgrade] Poster download failed for {safe_title_year(item)}, skipping...")
            summary["skipped"] = summary.get("skipped", 0) + 1
    finally:
        # Clean up temp file if it still exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    # Track processed asset
    existing_assets.add(str(asset_path.resolve()))

def process_season_poster(tmdb_id, season_number, item, library_name, existing_assets, summary):
    """
    Download and process the best poster for a specific TV season.

    Fetches season poster data from TMDb, selects the best poster, downloads it,
    and upgrades the asset if needed. Updates summary counters and asset tracking.

    Args:
        tmdb_id (int): The TMDb ID for the TV show.
        season_number (int): The season number.
        item: The Plex item object.
        library_name (str): The name of the Plex library.
        existing_assets (set): Set of asset paths already processed.
        summary (dict): Dictionary for tracking summary statistics.

    Returns:
        None
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
        logging.info(f"[Media Fetch] No posters found or season not found for {safe_title_year(item)} Season {season_number}. Skipping...")
        return

    response_data = response.json()
    images = response_data.get("images", {}).get("posters", [])
    if not images:
        logging.info(f"[Media Fetch] No season posters available for {safe_title_year(item)} Season {season_number}.")
        return

    fallback_languages = config["tmdb"].get("fallback_languages", [])
    best = get_best_poster(images, preferred_language=preferred_language, fallback_languages=fallback_languages)
    if not best:
        logging.info(f"[Media Fetch] No suitable season poster found for {safe_title_year(item)} Season {season_number}. Skipping...")
        return

    parent_dir = get_plex_show_directory(item)
    asset_path = Path(config["assets"]["assets_path"]) / library_name / parent_dir / config["assets"].get("season_filename", "Season{season_number:02}.jpg").format(season_number=season_number)
    temp_path = generate_temp_path(library_name)
    cache_key = f"tv:{item.title}:{item.year}:season{season_number}"

    try:
        if config.get("dry_run", False):
            logging.info(f"[Dry Run] Would process season poster for {safe_title_year(item)} Season {season_number}")
            summary["skipped"] = summary.get("skipped", 0) + 1
            existing_assets.add(str(asset_path.resolve()))
            return

        # Download poster and check if upgrade is needed
        if download_poster(best["file_path"], temp_path, item) and temp_path.exists():
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item, season_number=season_number):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Media Upgrade] Season poster upgraded for {safe_title_year(item)} Season {season_number}")
                summary["updated"] = summary.get("updated", 0) + 1
                # Update cache with new vote_average
                with cache_lock:
                    if cache_key in tmdb_cache and isinstance(tmdb_cache[cache_key], dict):
                        tmdb_cache[cache_key]["vote_average"] = best.get("vote_average")
                    else:
                        tmdb_cache[cache_key] = {
                            "tmdb_id": tmdb_id,
                            "vote_average": best.get("vote_average")
                        }
                    save_cache(tmdb_cache)
            else:
                temp_path.unlink(missing_ok=True)
                logging.info(f"[Media Upgrade] No season poster upgrade needed for {safe_title_year(item)} Season {season_number}")
                summary["skipped"] = summary.get("skipped", 0) + 1
        else:
            logging.warning(f"[Media Upgrade] Failed to download season poster for {safe_title_year(item)} Season {season_number}. Skipping...")
            summary["skipped"] = summary.get("skipped", 0) + 1
    finally:
        # Clean up temp file if it still exists
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    # Track processed asset
    existing_assets.add(str(asset_path.resolve()))