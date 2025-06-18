import logging
import hashlib
import uuid
from pathlib import Path
from helper.config import load_config
from helper.cache import load_cache
from helper.tmdb import tmdb_api_request

config = load_config()

def get_best_poster(
    images,
    preferred_language="en",
    fallback=None,
    preferred_vote=None,
    preferred_width=None,
    preferred_height=None,
    relaxed_vote=None,
    min_width=None,
    min_height=None,
    library_type=None
):
    """
    Select the best poster image from a list based on language and quality preferences.
    """
    if not images:
        logging.debug(f"[{library_type}] No images available to select the best poster.")
        return None

    # Ensure fallback is always a list
    if fallback is None:
        fallback = config["tmdb"].get("fallback", [])
    if isinstance(fallback, str):
        fallback = [fallback]
    language_priority = [preferred_language] + fallback

    logging.debug(f"[{library_type}] Language priority for posters: {language_priority}")

    # Use config defaults if not provided
    poster_sel = config["poster_settings"]
    preferred_vote = preferred_vote if preferred_vote is not None else poster_sel["preferred_vote"]
    preferred_width = preferred_width if preferred_width is not None else poster_sel["preferred_width"]
    preferred_height = preferred_height if preferred_height is not None else poster_sel["preferred_height"]
    relaxed_vote = relaxed_vote if relaxed_vote is not None else poster_sel["vote_relaxed"]
    min_width = min_width if min_width is not None else poster_sel["min_width"]
    min_height = min_height if min_height is not None else poster_sel["min_height"]

    # Filter images by language priority
    for lang in language_priority:
        language_filtered = [img for img in images if img.get("iso_639_1") == lang]
        logging.debug(f"[{library_type}] Found {len(language_filtered)} posters for language '{lang}'")
        if language_filtered:
            images_to_consider = language_filtered
            break
    else:
        images_to_consider = images
        logging.debug(f"[{library_type}] No posters found for preferred or fallback languages, considering all posters.")

    # High quality filter
    filtered = [
        img for img in images_to_consider
        if img.get("vote_average", 0) >= preferred_vote and
           img.get("width", 0) >= preferred_width and
           img.get("height", 0) >= preferred_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        logging.debug(f"[{library_type}] Selected high-quality poster: {best}")
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
        logging.debug(f"[{library_type}] Selected fallback poster: {best}")
        return best

    # Fallback: poster with minimum width/height
    filtered = [
        img for img in images_to_consider
        if img.get("width", 0) >= min_width and img.get("height", 0) >= min_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: x["width"] * x["height"])
        logging.debug(f"[{library_type}] Selected poster meeting minimum size as fallback: {best}")
        return best

    # Fallback: poster with the largest width and height
    if images_to_consider:
        best = max(images_to_consider, key=lambda x: x["width"] * x["height"])
        logging.debug(f"[{library_type}] Selected poster meeting largest size as final fallback: {best}")
        return best
    else:
        logging.warning(f"[{library_type}] No posters available at all after all filters.")
        return None

def get_best_background(
    images,
    preferred_vote=None,
    preferred_width=None,
    preferred_height=None,
    relaxed_vote=None,
    min_width=None,
    min_height=None,
    library_type=None
):
    """
    Select the best background image from a list based on quality preferences.
    """
    if not images:
        logging.debug(f"[{library_type}] No images available to select the best background.")
        return None

    bg_sel = config["background_settings"]
    preferred_vote = preferred_vote if preferred_vote is not None else bg_sel["preferred_vote"]
    preferred_width = preferred_width if preferred_width is not None else bg_sel["preferred_width"]
    preferred_height = preferred_height if preferred_height is not None else bg_sel["preferred_height"]
    relaxed_vote = relaxed_vote if relaxed_vote is not None else bg_sel["vote_relaxed"]
    min_width = min_width if min_width is not None else bg_sel["min_width"]
    min_height = min_height if min_height is not None else bg_sel["min_height"]

    # High quality filter
    filtered = [
        img for img in images
        if img.get("vote_average", 0) >= preferred_vote and
           img.get("width", 0) >= preferred_width and
           img.get("height", 0) >= preferred_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        logging.debug(f"[{library_type}] Selected high-quality background: {best}")
        return best

    # Relaxed filter
    filtered = [
        img for img in images
        if img.get("vote_average", 0) >= relaxed_vote and
           img.get("width", 0) >= min_width and
           img.get("height", 0) >= min_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        logging.debug(f"[{library_type}] Selected fallback background: {best}")
        return best

    # Fallback: any background
    best = max(images, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
    logging.debug(f"[{library_type}] Selected any available background as final fallback: {best}")
    return best

def smart_meta_update(existing_metadata, new_metadata):
    """
    Compare existing and new metadata, returning a list of changed fields.
    """
    changed_fields = []
    for key, new_value in new_metadata.items():
        existing_value = existing_metadata.get(key)
        if isinstance(new_value, list):
            if not isinstance(existing_value, list):
                changed_fields.append(key)
            else:
                normalized_existing = sorted([str(item) for item in existing_value])
                normalized_new = sorted([str(item) for item in new_value])
                if normalized_existing != normalized_new:
                    changed_fields.append(key)
        elif isinstance(new_value, dict):
            if not isinstance(existing_value, dict):
                changed_fields.append(key)
            else:
                nested_changes = smart_meta_update(existing_value, new_value)
                if nested_changes:
                    changed_fields.append(key)
        else:
            if str(existing_value or "").strip() != str(new_value or "").strip():
                changed_fields.append(key)
    return changed_fields

def smart_asset_upgrade(
    asset_path,
    new_image_data,
    new_image_path=None,
    cache_key=None,
    season_number=None,
    asset_type="poster",
    library_type=None,
    meta=None
):
    from PIL import Image
    """
    Determine if the new poster image should replace the existing one.
    """
    new_width = new_image_data.get("width", 0)
    new_height = new_image_data.get("height", 0)
    new_votes = new_image_data.get("vote_average", 0)
    title_year = meta.get("title_year") if meta else None
    label = title_year or "Unknown"
    if season_number is not None:
        label = f"{label} Season {season_number}"
    if library_type == "Collection":
        if asset_type == "collection":
            vote_threshold = config["poster_settings"].get("vote_threshold", 5.0)
            cache_key_name = "collection_average"
        elif asset_type == "collection_background":
            vote_threshold = config["background_settings"].get("vote_threshold", 5.0)
            cache_key_name = "collection_bg_average"
        else:
            vote_threshold = config["poster_settings"].get("vote_threshold", 5.0)
            cache_key_name = "collection_average"
    else:
        if asset_type == "background":
            vote_threshold = config["background_settings"].get("vote_threshold", 5.0)
            cache_key_name = "bg_average"
        else:
            vote_threshold = config["poster_settings"].get("vote_threshold", 5.0)
            cache_key_name = "poster_average"

    # Compare to cached vote_average if available
    cached_votes = 0
    if cache_key:
        cache = load_cache()
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            cached_votes = cached.get(cache_key_name, 0)

    # Only upgrade if new vote_average is higher than cached,
    # Or if there is no cached value and new_votes meets threshold
    if new_votes > cached_votes:
        logging.debug(f"[{library_type}] {label} upgraded based on vote average: {new_votes} (Cached: {cached_votes}, Threshold: {vote_threshold})")
        return not config["settings"].get("dry_run", False)
    elif cached_votes == 0 and new_votes >= vote_threshold:
        logging.debug(f"[{library_type}] {label} upgraded based on vote average threshold: {new_votes} (Threshold: {vote_threshold})")
        return not config["settings"].get("dry_run", False)

    # If no existing poster, always upgrade
    if not asset_path.exists():
        logging.info(f"[{library_type}] No existing poster for {label}. Downloading new poster.")
        return not config["settings"].get("dry_run", False)

    # If new image file exists, compare dimensions
    if new_image_path and new_image_path.exists():
        try:
            with Image.open(new_image_path) as img:
                existing_width, existing_height = img.size
        except Exception as e:
            logging.warning(f"[{library_type}] Failed to read temp image for comparison: {e}")
            return not config["settings"].get("dry_run", False)
    else:
        logging.debug(f"[{library_type}] No image provided for comparison. Skipping detailed check.")
        return False

    # Prefer new image if it's larger than existing
    if new_width > existing_width or new_height > existing_height:
        logging.debug(
            f"[{library_type}] {label}: New {new_width}x{new_height}, "
            f"Existing {existing_width}x{existing_height}"
        )
        return not config["settings"].get("dry_run", False)

    logging.debug(f"[{library_type}] No upgrade needed for {label}. Existing image meets criteria.")
    return False

def asset_temp_path(library_name, extension="jpg"):
    """
    Generate a temporary file path for storing assets.
    """
    assets_path = Path(config["assets"]["path"])
    temp_dir = assets_path / library_name
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_filename = f"temp_{uuid.uuid4().hex}.{extension}"
    return temp_dir / temp_filename

async def save_poster(image_content, save_path, library_type=None, meta=None):
    import asyncio
    """
    Save poster image content to disk, avoiding unnecessary overwrites.
    """
    title_year = meta.get("title_year") if meta else None
    try:
        new_checksum = hashlib.md5(image_content).hexdigest()

        # If file exists, compare checksums to avoid unnecessary writes
        if save_path.exists():
            with open(save_path, "rb") as existing_file:
                existing_checksum = hashlib.md5(existing_file.read()).hexdigest()
            if existing_checksum == new_checksum:
                logging.info(f"[{library_type}] No changes detected for {title_year}. Skipping save.")
                return
            else:
                logging.info(f"[{library_type}] Checksum difference detected for {title_year}. Proceeding to update.")
        if config["settings"].get("dry_run", False):
            logging.info(f"[Dry Run] Would save poster for {title_year}")
            return
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Use async file write for compatibility with async context
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, save_path.write_bytes, image_content)
        logging.debug(f"[{library_type}] Poster saved successfully for {title_year}")

    except Exception as e:
        logging.warning(f"[{library_type}] Failed to save poster for {title_year}: {e}")

async def download_poster(image_path, save_path, session=None, meta=None, library_type=None):
    """
    Download a poster image from TMDb and save it to the specified path.
    """
    title_year = meta.get("title_year") if meta else None
    if session is None:
        raise ValueError("An aiohttp session must be passed to download_poster")
    try:
        url = f"https://image.tmdb.org/t/p/original{image_path}"
        logging.debug(f"{library_type} Downloading {title_year} poster from URL: {url}")
        if save_path.exists() and not config["settings"].get("dry_run", False):
            logging.info(f"{library_type} Poster {title_year} already exists. Skipping download.")
            return True
        if config["settings"].get("dry_run", False):
            logging.info(f"[Dry Run] Would download {title_year} from {url}")
            return True 
        response_content = await tmdb_api_request(url, raw=True, cache=False, session=session)
        if not response_content:
            logging.warning(f"{library_type} Failed to download image for {title_year}")
            return False
        try:
            await save_poster(response_content, save_path, library_type=library_type, meta=meta)
            if save_path.exists():
                logging.info(f"{library_type} Downloaded poster for {title_year}")
                return True
            else:
                logging.warning(f"{library_type} Poster file was not created for {title_year} at {save_path}")
                return False
        except Exception as e:
            logging.warning(f"{library_type} Failed to save poster for {title_year}: {e}")
            return False

    except Exception as e:
        logging.warning(f"{library_type} Failed to process poster download for {title_year}: {e}")
        return False
    
def get_asset_path(meta, asset_type="poster", season_number=None, collection_name=None):
    """
    Returns the correct asset path based on config.
    """
    mode = config["assets"].get("mode", "kometa")
    library_type = meta.get("library_type")
    show_path = meta.get("show_path")
    movie_path = meta.get("movie_path")
    assets_path = Path(config["assets"]["path"])

    if asset_type in ("collection", "collection_background"):
        if mode != "kometa":
            return None
        if not collection_name:
            raise ValueError("collection_name must be provided for collection asset types in kometa mode.")
        base = assets_path / library_type / collection_name
        if asset_type == "collection":
            return base / "poster.jpg"
        elif asset_type == "collection_background":
            return base / "fanart.jpg"

    if mode == "plex":
        if asset_type == "poster":
            if library_type == "movie":
                return Path(meta["movie_dir"]) / "poster.jpg"
            elif library_type in ("show", "tv"):
                return Path(meta["show_dir"]) / "poster.jpg"
        elif asset_type == "background":
            if library_type == "movie":
                return Path(meta["movie_dir"]) / "fanart.jpg"
            elif library_type in ("show", "tv"):
                return Path(meta["show_dir"]) / "fanart.jpg"
        elif asset_type == "season" and season_number is not None:
            return Path(meta["show_dir"]) / f"Season {season_number:02}" / f"Season{season_number:02}.jpg"
    else:
        assets_path = Path(config["assets"]["path"])
        if asset_type == "poster":
            if library_type == "movie":
                return assets_path / library_type / movie_path / "poster.jpg"
            elif library_type in ("show", "tv"):
                return assets_path / library_type / show_path / "poster.jpg"
        elif asset_type == "background":
            if library_type == "movie":
                return assets_path / library_type / movie_path / "fanart.jpg"
            elif library_type in ("show", "tv"):
                return assets_path / library_type / show_path / "fanart.jpg"
        elif asset_type == "season" and season_number is not None:
            return assets_path / library_type / show_path / f"Season{season_number:02}.jpg"
    return None