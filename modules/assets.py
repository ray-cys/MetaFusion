import logging
import hashlib
import uuid
from pathlib import Path
from helper.config import load_config
from helper.plex import safe_title_year
from helper.tmdb import tmdb_cache

config = load_config()

def get_best_poster(
    images,
    preferred_language="en",
    fallback_languages=None,
    preferred_vote=None,
    preferred_width=None,
    preferred_height=None,
    relaxed_vote=None,
    min_width=None,
    min_height=None
):
    """
    Select the best poster image from a list based on language and quality preferences.
    """
    if not images:
        logging.debug("[Assets Selection] No images available to select the best poster.")
        return None

    fallback_languages = fallback_languages or []
    language_priority = [preferred_language] + fallback_languages

    # Use config defaults if not provided
    poster_sel = config["poster_selection"]
    preferred_vote = preferred_vote if preferred_vote is not None else poster_sel["preferred_vote"]
    preferred_width = preferred_width if preferred_width is not None else poster_sel["preferred_width"]
    preferred_height = preferred_height if preferred_height is not None else poster_sel["preferred_height"]
    relaxed_vote = relaxed_vote if relaxed_vote is not None else poster_sel["vote_relaxed"]
    min_width = min_width if min_width is not None else poster_sel["min_width"]
    min_height = min_height if min_height is not None else poster_sel["min_height"]

    # Filter images by language priority
    for lang in language_priority:
        language_filtered = [img for img in images if img.get("iso_639_1") == lang]
        if language_filtered:
            images_to_consider = language_filtered
            break
    else:
        images_to_consider = images

    # High quality filter
    filtered = [
        img for img in images_to_consider
        if img.get("vote_average", 0) >= preferred_vote and
           img.get("width", 0) >= preferred_width and
           img.get("height", 0) >= preferred_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        logging.debug(f"[Assets Selection] Selected high-quality poster: {best}")
        return best

    # Relaxed filter
    filtered = [
        img for img in images_to_consider
        if img.get("vote_average", 0) >= relaxed_vote and
           img.get("width", 0) >= min_width and
           img.get("height", 0) >= min_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        logging.debug(f"[Assets Selection] Selected fallback poster: {best}")
        return best

    # Fallback: any poster
    best = max(images_to_consider, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
    logging.debug(f"[Assets Selection] Selected any available poster as final fallback: {best}")
    return best

def download_poster(image_path, save_path, item=None):
    """
    Download a poster image from TMDb and save it to the specified path.
    """
    try:
        url = f"https://image.tmdb.org/t/p/original{image_path}"
        logging.debug(f"[Assets Download] Downloading {safe_title_year(item)} poster from URL: {url}")

        # Skip download if file already exists and not in dry run mode
        if save_path.exists() and not config.get("dry_run", False):
            logging.info(f"[Assets Download] Poster {safe_title_year(item)} already exists. Skipping download.")
            return True

        # Dry run mode: simulate download
        if config.get("dry_run", False):
            logging.info(f"[Assets Dry Run] Would download {safe_title_year(item)} from {url}")
            return True 

        from helper.tmdb import safe_get_with_retries 
        response = safe_get_with_retries(url)
        if not response or response.status_code != 200:
            logging.warning(f"[Assets Download] Failed to download image for {safe_title_year(item)}")
            return False

        try:
            save_poster(response.content, save_path, item)
            logging.debug(f"[Assets Download] Downloaded poster for {safe_title_year(item)}")
            return True
        except Exception as e:
            logging.warning(f"[Assets Download] Failed to save poster for {safe_title_year(item)}: {e}")
            return False

    except Exception as e:
        logging.warning(f"[Assets Download] Failed to process poster download for {safe_title_year(item)}: {e}")
        return False

def should_upgrade(asset_path, new_image_data, new_image_path=None, cache_key=None, item=None, season_number=None):
    from PIL import Image
    """
    Determine if the new poster image should replace the existing one.
    """
    new_width = new_image_data.get("width", 0)
    new_height = new_image_data.get("height", 0)
    new_votes = new_image_data.get("vote_average", 0)
    label = safe_title_year(item)
    if season_number is not None:
        label = f"{label} Season {season_number}"

    vote_threshold = config["poster_selection"].get("vote_average_threshold", 5.0)

    # Compare to cached vote_average if available
    cached_votes = 0
    if cache_key and cache_key in tmdb_cache:
        cached = tmdb_cache[cache_key]
        if isinstance(cached, dict):
            cached_votes = cached.get("vote_average", 0)

    # Only upgrade if new vote_average is higher than cached,
    # or if there is no cached value and new_votes meets threshold
    if new_votes > cached_votes:
        logging.debug(f"[Assets Recommendation] {label} upgraded based on vote average: {new_votes} (Cached: {cached_votes}, Threshold: {vote_threshold})")
        return not config.get("dry_run", False)
    elif cached_votes == 0 and new_votes >= vote_threshold:
        logging.debug(f"[Assets Recommendation] {label} upgraded based on vote average threshold: {new_votes} (Threshold: {vote_threshold})")
        return not config.get("dry_run", False)

    # If no existing poster, always upgrade
    if not asset_path.exists():
        logging.info(f"[Assets Download] No existing poster for {label}. Downloading new poster.")
        return not config.get("dry_run", False)

    # If new image file exists, compare dimensions
    if new_image_path and new_image_path.exists():
        try:
            with Image.open(new_image_path) as img:
                existing_width, existing_height = img.size
        except Exception as e:
            logging.warning(f"[Assets Checksum] Failed to read temp image for comparison: {e}")
            return not config.get("dry_run", False)
    else:
        logging.debug(f"[Assets Checksum] No new_image_path provided for comparison. Skipping detailed check.")
        return False

    # Prefer new image if it's larger than existing
    if new_width > existing_width or new_height > existing_height:
        logging.debug(
            f"[Assets Recommendation] {label}: New {new_width}x{new_height}, "
            f"Existing {existing_width}x{existing_height}"
        )
        return not config.get("dry_run", False)

    logging.debug(f"[Assets Upgrade] No upgrade needed for {safe_title_year(item)}. Existing image meets criteria.")
    return False

def generate_temp_path(library_name, extension="jpg"):
    """
    Generate a temporary file path for storing a poster image.
    """
    assets_path = Path(config["assets"]["assets_path"])
    temp_dir = assets_path / library_name
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_filename = f"temp_{uuid.uuid4().hex}.{extension}"
    return temp_dir / temp_filename

def save_poster(image_content, save_path, item=None):
    """
    Save poster image content to disk, avoiding unnecessary overwrites.
    """
    try:
        new_checksum = hashlib.md5(image_content).hexdigest()

        # If file exists, compare checksums to avoid unnecessary writes
        if save_path.exists():
            with open(save_path, "rb") as existing_file:
                existing_checksum = hashlib.md5(existing_file.read()).hexdigest()
            
            if existing_checksum == new_checksum:
                logging.info(f"[Assets Save] No changes detected for {safe_title_year(item)}. Skipping save.")
                return
            else:
                logging.info(f"[Assets Save] Checksum difference detected for {safe_title_year(item)}. Proceeding to update.")

        if config.get("dry_run", False):
            logging.info(f"[Assets Dry Run] Would save poster for {safe_title_year(item)}")
            return

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "wb") as f:
            f.write(image_content)

        logging.debug(f"[Assets Save] Poster saved successfully for {safe_title_year(item)}")

    except Exception as e:
        logging.warning(f"[Assets Save] Failed to save poster for {safe_title_year(item)}: {e}")