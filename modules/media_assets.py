import logging
import shutil
from pathlib import Path
from helper.config import load_config
from helper.tmdb import (
    update_meta_cache, download_poster, tmdb_api_request
)
from helper.plex import get_plex_movie_directory, get_plex_show_directory, safe_title_year
from helper.logger import human_readable_size
from modules.utils import (
    should_upgrade, generate_temp_path, get_best_poster, get_best_background
)

config = load_config()

async def process_poster_for_media(
    media_type, 
    tmdb_id, 
    item, 
    library_name, 
    existing_assets, 
    episode_cache=None, 
    movie_cache=None, 
    session=None,
    ):
    """
    Download and process the best poster for a movie or TV show item.
    """
    parent_dir = (await get_plex_show_directory(item, _episode_cache=episode_cache)) if media_type == "tv" else (await get_plex_movie_directory(item, _movie_cache=movie_cache))
    asset_path = Path(config["assets_path"]) / library_name / parent_dir / "poster.jpg"
    asset_exists = asset_path.exists()
    if asset_exists:
        existing_assets.add(str(asset_path.resolve()))
        return 0, 0

    logging.debug(f"[Script State] Processing Movies & TV Shows poster: TMDb ID {tmdb_id}")
    preferred_language = config["tmdb"].get("language", "en").split("-")[0]
    response_data = await tmdb_api_request(
        f"{media_type}/{tmdb_id}",
        params={
            "append_to_response": "images",
            "language": config["tmdb"].get("language", "en"),
            "include_image_language": f"{preferred_language},null"
        },
         session=session
    )

    if not response_data:
        logging.warning(f"[Assets] Failed to fetch poster data for {safe_title_year(item)}. Skipping...")
        return 0, 0

    images = response_data.get("images", {}).get("posters", [])
    fallback = config["tmdb"].get("fallback", [])
    best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)

    if not best:
        logging.info(f"[Assets] No suitable poster found for {safe_title_year(item)}. Skipping...")
        return 0, 0

    temp_path = generate_temp_path(library_name)
    title = getattr(item, "title", "Unknown")
    year = getattr(item, "year", "Unknown")
    media_type = media_type.lower() if "media_type" in locals() else "tv"
    if media_type == "show":
        media_type = "tv"
    elif media_type != "movie" and media_type != "tv":
        media_type = "movie"
    cache_key = f"{media_type}:{title}:{year}"
    
    downloaded_size = 0
    downloaded_count = 0
    try:
        if config.get("dry_run", False):
            logging.info(f"[Dry Run] Would process poster for {safe_title_year(item)}")
            existing_assets.add(str(asset_path.resolve()))
            return 0, 0

        if await download_poster(best["file_path"], temp_path, item, session=session) and temp_path.exists():
            downloaded_size = temp_path.stat().st_size
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Assets] Poster upgraded for {safe_title_year(item)}. Filesize: {human_readable_size(downloaded_size)}")
                downloaded_count += 1
                # Update cache with new vote_average
                update_meta_cache(cache_key, tmdb_id, title, year, media_type, poster_average=best.get("vote_average"))
            else:
                logging.info(f"[Assets] No poster upgrade needed for {safe_title_year(item)}")
                temp_path.unlink(missing_ok=True)
        else:
            logging.warning(f"[Assets] Poster download failed for {safe_title_year(item)}, skipping...")
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    existing_assets.add(str(asset_path.resolve()))
    return downloaded_size, downloaded_count

async def process_season_poster(tmdb_id, season_number, item, library_name, existing_assets, episode_cache=None, session=None):
    """
    Download and process the best poster for a specific TV season.
    """
    parent_dir = (await get_plex_show_directory(item, _episode_cache=episode_cache))
    asset_path = Path(config["assets_path"]) / library_name / parent_dir / f"Season{season_number:02}.jpg"
    asset_exists = asset_path.exists()
    if asset_exists:
        existing_assets.add(str(asset_path.resolve()))
        return 0, 0
    
    logging.debug(f"[Script State] Processing Season poster: TMDb ID {tmdb_id}, Season {season_number}")
    preferred_language = config["tmdb"].get("language", "en").split("-")[0]
    response_data = await tmdb_api_request(
        f"tv/{tmdb_id}/season/{season_number}",
        params={
            "append_to_response": "images",
            "language": config["tmdb"].get("language", "en"),
            "include_image_language": f"{preferred_language},null"
        },
        session=session 
    )

    if not response_data:
        logging.info(f"[Assets] No posters found or season not found for {safe_title_year(item)} Season {season_number}. Skipping...")
        return 0, 0

    images = response_data.get("images", {}).get("posters", [])
    if not images:
        logging.info(f"[Assets] No season posters available for {safe_title_year(item)} Season {season_number}.")
        return 0, 0

    fallback = config["tmdb"].get("fallback", [])
    best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)
    if not best:
        logging.info(f"[Assets] No suitable season poster found for {safe_title_year(item)} Season {season_number}. Skipping...")
        return 0, 0

    temp_path = generate_temp_path(library_name)
    title = getattr(item, "title", "Unknown")
    year = getattr(item, "year", "Unknown")
    media_type = "tv"
    cache_key = f"tv:{title}:{year}:season{season_number}"
    
    downloaded_size = 0
    downloaded_count = 0
    try:
        if config.get("dry_run", False):
            logging.info(f"[Dry Run] Would process season poster for {safe_title_year(item)} Season {season_number}")
            existing_assets.add(str(asset_path.resolve()))
            return 0, 0

        if await download_poster(best["file_path"], temp_path, item, session=session) and temp_path.exists():
            downloaded_size = temp_path.stat().st_size
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item, season_number=season_number):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Assets] Season poster upgraded for {safe_title_year(item)} Season {season_number}. Filesize: {human_readable_size(downloaded_size)}")
                downloaded_count = 1
                update_meta_cache(cache_key, tmdb_id, title, year, media_type, poster_average=best.get("vote_average"))
            else:
                temp_path.unlink(missing_ok=True)
                logging.info(f"[Assets] No season poster upgrade needed for {safe_title_year(item)} Season {season_number}")
        else:
            logging.warning(f"[Assets] Failed to download season poster for {safe_title_year(item)} Season {season_number}. Skipping...")
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    existing_assets.add(str(asset_path.resolve()))
    return downloaded_size, downloaded_count

async def process_background_for_media(media_type, tmdb_id, item, library_name, existing_assets, episode_cache=None, movie_cache=None, session=None):
    """
    Download and process the best background (fanart) for a Movie or TV Show item.
    """
    parent_dir = (await get_plex_show_directory(item, _episode_cache=episode_cache)) if media_type == "tv" else (await get_plex_movie_directory(item, _movie_cache=movie_cache))
    asset_path = Path(config["assets_path"]) / library_name / parent_dir / "fanart.jpg"
    asset_exists = asset_path.exists()
    if asset_exists:
        existing_assets.add(str(asset_path.resolve()))
        return 0, 0
    
    logging.debug(f"[Script State] Processing Movies & TV Shows background: TMDb ID {tmdb_id}")
    response_data = await tmdb_api_request(
        f"{media_type}/{tmdb_id}",
        params={
            "append_to_response": "images",
        },
        session=session 
    )

    if not response_data:
        logging.warning(f"[Assets] Failed to fetch background data for {safe_title_year(item)}. Skipping...")
        return 0, 0

    images = response_data.get("images", {}).get("backdrops", [])
    if not images:
        logging.info(f"[Assets] No backgrounds found for {safe_title_year(item)}. Skipping...")
        return 0, 0

    best = get_best_background(images)
    if not best:
        logging.info(f"[Assets] No suitable background found for {safe_title_year(item)}. Skipping...")
        return 0, 0

    temp_path = generate_temp_path(library_name)
    title = getattr(item, "title", "Unknown")
    year = getattr(item, "year", "Unknown")
    media_type = media_type.lower() if "media_type" in locals() else "tv"
    if media_type == "show":
        media_type = "tv"
    elif media_type != "movie" and media_type != "tv":
        media_type = "movie"
    cache_key = f"{media_type}:{title}:{year}"

    downloaded_size = 0
    downloaded_count = 0
    try:
        if config.get("dry_run", False):
            logging.info(f"[Dry Run] Would process background for {safe_title_year(item)}")
            existing_assets.add(str(asset_path.resolve()))
            return 0, 0

        if await download_poster(best["file_path"], temp_path, item, session=session) and temp_path.exists():
            downloaded_size = temp_path.stat().st_size
            if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=item):
                asset_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(temp_path, asset_path)
                logging.info(f"[Assets] Background upgraded for {safe_title_year(item)}. Filesize: {human_readable_size(downloaded_size)}")
                downloaded_count = 1
                update_meta_cache(cache_key, tmdb_id, title, year, media_type, bg_average=best.get("vote_average"))
            else:
                temp_path.unlink(missing_ok=True)
                logging.info(f"[Assets] No background upgrade needed for {safe_title_year(item)}")
        else:
            logging.warning(f"[Assets] Background download failed for {safe_title_year(item)}, skipping...")
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    existing_assets.add(str(asset_path.resolve()))
    return downloaded_size, downloaded_count