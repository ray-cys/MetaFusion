import asyncio
import hashlib
import uuid
from pathlib import Path
from helper.config import load_config_file
from helper.cache import load_cache
from helper.tmdb import tmdb_api_request

def smart_meta_update(existing_metadata, new_metadata):
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

def get_meta_field(data, field, default=None, path=None):
    try:
        if path:
            for key in path:
                data = data.get(key, {})
        return data.get(field, default)
    except Exception:
        return default
    
def get_best_poster(
    config, images, preferred_language="en", fallback=None, prefer_vote=None, max_width=None,
    max_height=None, relaxed_vote=None, min_width=None, min_height=None, is_collection=False,
):
    if not images:
        return None
    if fallback is None:
        fallback = config["tmdb"].get("fallback", [])
    if isinstance(fallback, str):
        fallback = [fallback]
    language_priority = [preferred_language] + fallback
    poster_sel = config["poster_set"]
    default_sel = poster_sel

    prefer_vote = prefer_vote if prefer_vote is not None else poster_sel.get("prefer_vote", default_sel.get("prefer_vote", 0))
    max_width = max_width if max_width is not None else poster_sel.get("max_width", default_sel.get("max_width", 0))
    max_height = max_height if max_height is not None else poster_sel.get("max_height", default_sel.get("max_height", 0))
    relaxed_vote = relaxed_vote if relaxed_vote is not None else poster_sel.get("vote_relaxed", default_sel.get("vote_relaxed", 0))
    min_width = min_width if min_width is not None else poster_sel.get("min_width", default_sel.get("min_width", 0))
    min_height = min_height if min_height is not None else poster_sel.get("min_height", default_sel.get("min_height", 0))
    
    for lang in language_priority:
        language_filtered = [img for img in images if img.get("iso_639_1") == lang]
        if language_filtered:
            images_to_consider = language_filtered
            break
    else:
        images_to_consider = images
        
    filtered = [
        img for img in images_to_consider
        if img.get("vote_average", 0) >= prefer_vote and
           img.get("width", 0) >= max_width and
           img.get("height", 0) >= max_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        return best
    
    filtered = [
        img for img in images_to_consider
        if img.get("vote_average", 0) >= relaxed_vote and
           img.get("width", 0) >= min_width and
           img.get("height", 0) >= min_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        return best
    
    filtered = [
        img for img in images_to_consider
        if img.get("width", 0) >= min_width and img.get("height", 0) >= min_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: x["width"] * x["height"])
        return best
    
    if images_to_consider:
        best = max(images_to_consider, key=lambda x: x["width"] * x["height"])
        return best
    
    if images_to_consider:
        return images_to_consider[0]
    else:
        return None

def get_best_background(
    config, images, prefer_vote=None, max_width=None, max_height=None, relaxed_vote=None,
    min_width=None, min_height=None, is_collection=False,
):
    if not images:
        return None
    
    bg_sel = config["background_set"]
    default_sel = bg_sel
        
    prefer_vote = prefer_vote if prefer_vote is not None else bg_sel.get("prefer_vote", default_sel.get("prefer_vote", 0))
    max_width = max_width if max_width is not None else bg_sel.get("max_width", default_sel.get("max_width", 0))
    max_height = max_height if max_height is not None else bg_sel.get("max_height", default_sel.get("max_height", 0))
    relaxed_vote = relaxed_vote if relaxed_vote is not None else bg_sel.get("vote_relaxed", default_sel.get("vote_relaxed", 0))
    min_width = min_width if min_width is not None else bg_sel.get("min_width", default_sel.get("min_width", 0))
    min_height = min_height if min_height is not None else bg_sel.get("min_height", default_sel.get("min_height", 0))
    
    filtered = [
        img for img in images
        if img.get("vote_average", 0) >= prefer_vote and
           img.get("width", 0) >= max_width and
           img.get("height", 0) >= max_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        return best
    
    filtered = [
        img for img in images
        if img.get("vote_average", 0) >= relaxed_vote and
           img.get("width", 0) >= min_width and
           img.get("height", 0) >= min_height
    ]
    if filtered:
        best = max(filtered, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        return best
    
    if images:
        best = max(images, key=lambda x: (x["vote_average"], x["width"] * x["height"]))
        return best

    if images:
        return images[0]
    else:
        return None

def smart_asset_upgrade(
    config, asset_path, new_image_data, new_image_path=None, cache_key=None,
    asset_type="poster", library_type=None, season_number=None
):
    from PIL import Image
    new_width = new_image_data.get("width", 0)
    new_height = new_image_data.get("height", 0)
    new_votes = new_image_data.get("vote_average", 0)
    if library_type == "Collection":
        if asset_type == "collection":
            vote_threshold = config["poster_set"].get("vote_threshold", 5.0)
            cache_key_name = "collection_average"
        elif asset_type == "collection_background":
            vote_threshold = config["background_set"].get("vote_threshold", 5.0)
            cache_key_name = "collection_bg_average"
        else:
            vote_threshold = config["poster_set"].get("vote_threshold", 5.0)
            cache_key_name = "collection_average"
    else:
        if asset_type == "background":
            vote_threshold = config["background_set"].get("vote_threshold", 5.0)
            cache_key_name = "bg_average"
        else:
            vote_threshold = config["poster_set"].get("vote_threshold", 5.0)
            cache_key_name = "poster_average"
    cached_votes = 0
    if cache_key:
        cache = load_cache()
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            cached_votes = cached.get(cache_key_name, 0)
    context = {
        "new_width": new_width,
        "new_height": new_height,
        "new_votes": new_votes,
        "cached_votes": cached_votes,
        "vote_threshold": vote_threshold,
        "asset_path_exists": asset_path.exists(),
        "new_image_path_exists": new_image_path.exists() if new_image_path else False
    }
    if new_votes > cached_votes:
        return True, "UPGRADE_VOTES", context
    elif cached_votes == 0 and new_votes >= vote_threshold:
        return True, "UPGRADE_THRESHOLD", context
    if not asset_path.exists():
        return True, "NO_EXISTING_ASSET", context
    if new_image_path and new_image_path.exists():
        try:
            with Image.open(new_image_path) as img:
                existing_width, existing_height = img.size
            context["existing_width"] = existing_width
            context["existing_height"] = existing_height
        except Exception as e:
            context["error"] = str(e)
            return False, "ERROR_IMAGE_COMPARE", context
    else:
        return False, "NO_IMAGE_FOR_COMPARE", context
    if new_width > context.get("existing_width", 0) or new_height > context.get("existing_height", 0):
        return True, "UPGRADE_DIMENSIONS", context
    return False, "NO_UPGRADE_NEEDED", context

async def download_poster(config, image_path, save_path, session=None, retries=3):
    if session is None:
        url = f"https://image.tmdb.org/t/p/original{image_path}"
        return False, url, None, "HTTP session failed"
    url = f"https://image.tmdb.org/t/p/original{image_path}"
    last_exception = None
    for attempt in range(retries):
        try:
            response_content = await tmdb_api_request(config, url, raw=True, cache=False, session=session)
            if response_content:
                result, error = await save_poster(response_content, save_path)
                if result is True or result == "ALREADY_UP_TO_DATE":
                    return True, url, 200, error
                else:
                    last_exception = Exception(error or "File not saved after download")
            else:
                last_exception = Exception("Empty response from TMDb")
        except Exception as e:
            last_exception = e
        await asyncio.sleep(1)
    status = getattr(last_exception, "status", None)
    return False, url, status, str(last_exception) if last_exception else None

def get_asset_path(config, meta, asset_type="poster", season_number=None, collection_name=None):
    mode = config["assets"].get("mode", "kometa")
    library_type = meta.get("library_type")
    show_path = meta.get("show_path")
    movie_path = meta.get("movie_path")
    assets_path = Path(config["assets"]["path"])

    if asset_type in ("collection", "collection_background"):
        if mode != "kometa":
            return None
        if not collection_name:
            raise ValueError("Collection name must be provided for asset in Kometa mode.")
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

def asset_temp_path(config, library_name, extension="jpg"):
    assets_path = Path(config["assets"]["path"])
    temp_dir = assets_path / library_name
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_filename = f"temp_{uuid.uuid4().hex}.{extension}"
    return temp_dir / temp_filename

async def save_poster(image_content, save_path):
    try:
        new_checksum = hashlib.md5(image_content).hexdigest()
        if save_path.exists():
            with open(save_path, "rb") as existing_file:
                existing_checksum = hashlib.md5(existing_file.read()).hexdigest()
            if existing_checksum == new_checksum:
                return "ALREADY_UP_TO_DATE", None
        save_path.parent.mkdir(parents=True, exist_ok=True)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, save_path.write_bytes, image_content)
        return True, None
    except Exception as e:
        return False, str(e)