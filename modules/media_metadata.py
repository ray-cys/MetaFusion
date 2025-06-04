import logging
from helper.tmdb import (
    tmdb_api_request, resolve_tmdb_id, update_tmdb_cache, tmdb_response_cache
)
from helper.plex import get_existing_plex_seasons_episodes
from helper.stats import log_metadata_completeness
from helper.config import load_config

config = load_config()

def smart_update_needed(existing_metadata, new_metadata):
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
                nested_changes = smart_update_needed(existing_value, new_value)
                if nested_changes:
                    changed_fields.append(key)
        else:
            if str(existing_value or "").strip() != str(new_value or "").strip():
                changed_fields.append(key)
    return changed_fields

def build_movie_metadata(plex_item, consolidated_metadata, dry_run=False, existing_yaml_data=None):
    import pycountry
    """
    Build and consolidate metadata for a movie Plex item using TMDb data.
    """
    title = getattr(plex_item, "title", "Unknown")
    year = getattr(plex_item, "year", "Unknown")
    full_title = f"{title} ({year})"
    # Try to resolve TMDb ID for the movie
    tmdb_id = resolve_tmdb_id(plex_item, title, year, "movie")
    imdb_id_for_mapping = ""
    mapping_id = ""

    # If no TMDb ID, try searching by title
    if not tmdb_id:
        search_results = tmdb_api_request(
            "search/movie",
            params={"query": title}
        )
        if search_results and search_results.get("results"):
            movie_id = search_results["results"][0].get("id")
            if movie_id:
                external_ids = tmdb_api_request(f"movie/{movie_id}/external_ids")
                if external_ids:
                    imdb_id_for_mapping = external_ids.get("imdb_id", "")
        mapping_id = imdb_id_for_mapping or ""
        if not mapping_id:
            logging.warning(f"[Metadata] No TMDb or IMDb ID found for {full_title}. Skipping...")
            return
    else:
        mapping_id = int(tmdb_id)

    # Try to get movie details from cache, else fetch from TMDb
    details_key = f"movie/{tmdb_id}"
    details = tmdb_response_cache.get(details_key)
    if not details:
        details = tmdb_api_request(
            details_key,
            params={
                "append_to_response": "credits,release_dates",
                "language": config.get("tmdb", {}).get("language", "en"),
                "region": config.get("tmdb", {}).get("region", "US")
            }
        )
        if details:
            tmdb_response_cache[details_key] = details
        else:
            logging.warning(f"[Metadata] No TMDb data found for {full_title}. Skipping...")
            return

    # Extract content rating (US certification)
    content_rating = next(
        (c.get("certification", "") 
        for country in details.get("release_dates", {}).get("results", []) 
        if country.get("iso_3166_1") == "US" 
        for c in country.get("release_dates", []) if c.get("certification")), ""
    )

    # Extract genres, studios, countries, etc.
    genres = [g.get("name", "") for g in details.get("genres", [])]
    production_companies = details.get("production_companies", [])
    studios = [c.get("name", "") for c in production_companies if c.get("name")]
    studio = ", ".join(studios) if studios else ""

    production_countries = details.get("production_countries", [])
    country_codes = [c.get("iso_3166_1", "") for c in production_countries if c.get("iso_3166_1")]
    countries = [
        getattr(pycountry.countries.get(alpha_2=code), "official_name", pycountry.countries.get(alpha_2=code).name)
        for code in country_codes if pycountry.countries.get(alpha_2=code)
    ]

    release_date = details.get("release_date", "")
    originally_available = release_date or ""
    runtime = details.get("runtime", None)

    # Collection/franchise info
    collection_info = details.get("belongs_to_collection")
    collection = collection_info.get("name", "") if collection_info else ""

    # Crew roles
    director_jobs = {"Director", "Co-Director", "Assistant Director"}
    writer_jobs = {"Writer", "Screenplay", "Story", "Creator", "Co-Writer", "Author", "Adaptation"}
    producer_jobs = {"Producer", "Executive Producer", "Associate Producer", "Co-Producer", "Line Producer", "Co-Executive Producer"}

    credits = details.get("credits", {})
    crew = credits.get("crew", []) or []
    cast = credits.get("cast", []) or []
    directors = [m.get("name", "") for m in crew if m.get("job") in director_jobs]
    writers = [m.get("name", "") for m in crew if m.get("job") in writer_jobs]
    producers = [m.get("name", "") for m in crew if m.get("job") in producer_jobs]
    top_cast = [c.get("name", "") for c in cast[:10]]

    # Build new metadata dictionary
    new_metadata = {
        "sort_title": title,
        "original_title": details.get("original_title", title),
        "originally_available": originally_available,
        "content_rating": content_rating,
        "studio": studio or "",
        "runtime": runtime,
        "tagline": details.get("tagline", ""),
        "summary": details.get("overview", ""),
        "country": countries or [],
        "genre": genres or [],
        "cast": top_cast or [],
        "director": directors or [],
        "writer": writers or [],
        "producer": producers or [],
        "collection": collection,
    }
    movie_expected_fields = [
        "sort_title", "original_title", "originally_available", "content_rating", "runtime",
        "studio", "tagline", "summary", "country", "genre", "cast", "director", "writer", "producer"
    ]
    # Log completeness of metadata
    percent = log_metadata_completeness("[Movie Metadata]", full_title, new_metadata, movie_expected_fields)

    # Smart update: check if anything changed
    if existing_yaml_data:
        existing_metadata = existing_yaml_data.get("metadata", {}).get(full_title, {})
        changes = smart_update_needed(existing_metadata, new_metadata)
        if not changes:
            logging.debug(f"[Metadata Update] No changes for {full_title}. Preserving existing metadata.")
            consolidated_metadata["metadata"][full_title] = existing_metadata
            return
        else:
            logging.debug(f"[Metadata Update] Fields changed for {full_title}: {changes}")
    
    metadata_entry = {
        "match": {
            "title": title,
            "year": year,
            "mapping_id": mapping_id
        },
        **new_metadata
    }

    if dry_run:
        logging.info(f"[Metadata Dry Run] Would build metadata for Movie: {full_title}")
        return

    # Save metadata and update cache
    consolidated_metadata["metadata"][full_title] = metadata_entry
    
    # Use standardized cache key format
    cache_key = f"movie:{title}:{year}"
    update_tmdb_cache(cache_key, tmdb_id, title, year, "movie")
    logging.debug(f"[Metadata] Movie metadata built and saved for {full_title} using TMDb ID {tmdb_id}.")
    return percent

def build_tv_metadata(plex_item, consolidated_metadata, dry_run=False, existing_yaml_data=None, season_cache=None, episode_cache=None):
    import pycountry
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
    """
    Build and consolidate metadata for a TV show Plex item using TMDb data.
    """
    title = getattr(plex_item, "title", "Unknown")
    year = getattr(plex_item, "year", "Unknown")
    full_title = f"{title} ({year})"
    # Try to resolve TMDb ID for the TV show
    tmdb_id = resolve_tmdb_id(plex_item, title, year, "tv")

    if not tmdb_id:
        logging.warning(f"[Metadata] No TMDb ID found for {full_title}. Skipping...")
        return

    try:
        tmdb_id_int = int(tmdb_id)
    except (ValueError, TypeError):
        logging.warning(f"[Metadata] Invalid TMDb ID format for {full_title}. Skipping...")
        return

    # Try to get show details from cache, else fetch from TMDb
    details_key = f"tv/{tmdb_id_int}"
    details = tmdb_response_cache.get(details_key)
    if not details:
        details = tmdb_api_request(
            details_key,
            params={
                "append_to_response": "credits,keywords,content_ratings,external_ids",
                "language": config.get("tmdb", {}).get("language", "en"),
                "region": config.get("tmdb", {}).get("region", "US")
            }
        )
        if details:
            tmdb_response_cache[details_key] = details
        else:
            logging.warning(f"[Metadata] No TMDb data found for {full_title}. Skipping...")
            return

    # Extract genres, studios, countries, etc.
    content_rating = next(
        (c.get("rating", "") for c in details.get("content_ratings", {}).get("results", [])
        if c.get("iso_3166_1") == "US"), ""
    )
    genres = [g.get("name", "") for g in details.get("genres", [])]
    studios = [n.get("name", "") for n in details.get("networks", []) if n.get("name")]
    studio = ", ".join(studios) if studios else ""
    originally_available = details.get("first_air_date", "") or ""

    country_codes = details.get("origin_country", [])
    countries = [
        getattr(pycountry.countries.get(alpha_2=code), "official_name", pycountry.countries.get(alpha_2=code).name)
        for code in country_codes if pycountry.countries.get(alpha_2=code)
    ]

    # Build new metadata dictionary for the show
    new_metadata = {
        "sort_title": title,
        "original_title": details.get("original_name", title),
        "originally_available": originally_available,
        "content_rating": content_rating,
        "studio": studio or "",
        "tagline": details.get("tagline", ""),
        "summary": details.get("overview", ""),
        "genre": genres or [],
        "country": countries or [],
    }

    # Get existing seasons/episodes from Plex
    existing_seasons_episodes = get_existing_plex_seasons_episodes(
        plex_item,
        _season_cache=season_cache,
        _episode_cache=episode_cache
    )
    seasons_data = {}

    def process_season(season_info):
        """
        Process metadata for a single season, including all episodes.
        """
        season_number = season_info.get("season_number")
        if season_number == 0 or season_number not in existing_seasons_episodes:
            return season_number, None

        season_key = f"tv/{tmdb_id_int}/season/{season_number}"
        season_details = tmdb_response_cache.get(season_key)
        if not season_details:
            season_details = tmdb_api_request(season_key)
            if season_details:
                tmdb_response_cache[season_key] = season_details
            else:
                logging.warning(f"[Metadata] No TMDb data found for Season {season_number} of {full_title}. Skipping...")
                return season_number, None

        # Extract show-level and season-level credits once
        show_crew = details.get("credits", {}).get("crew", []) or []
        show_cast = details.get("credits", {}).get("cast", []) or []
        season_crew = season_details.get("credits", {}).get("crew", []) or []
        season_cast = season_details.get("credits", {}).get("cast", []) or []

        ep_director_jobs = {"Director", "Co-Director", "Assistant Director"}
        ep_writer_jobs = {"Writer", "Screenplay", "Story", "Creator", "Co-Writer", "Author", "Adaptation"}

        episodes = {}
        episode_metadata_list = []
        for episode in season_details.get("episodes", []):
            ep_num = episode.get("episode_number")
            if ep_num not in existing_seasons_episodes[season_number]:
                continue
            ep_crew = episode.get("crew", []) or season_crew or show_crew
            ep_credits = episode.get("credits", {}) or {}
            ep_cast = ep_credits.get("cast", []) or season_cast or show_cast
            ep_guest_stars = ep_credits.get("guest_stars", []) or []

            ep_directors = [m.get("name", "") for m in ep_crew if m.get("job") in ep_director_jobs]
            ep_writers = [m.get("name", "") for m in ep_crew if m.get("job") in ep_writer_jobs]
            ep_cast = [c.get("name", "") for c in ep_cast[:10]]
            ep_guest_stars = [g.get("name", "") for g in ep_guest_stars[:5]]
            ep_air_date = episode.get("air_date", "") or ""
            ep_runtime = episode.get("runtime", None)

            episode_dict = {
                "title": episode.get("name", ""),
                "sort_title": episode.get("name", ""),
                "originally_available": ep_air_date,
                "runtime": ep_runtime,
                "summary": episode.get("overview", ""),
                "cast": ep_cast or [],
                "guest": ep_guest_stars or [],
                "director": ep_directors or [],
                "writer": ep_writers or [],
            }
            episode_metadata_list.append((ep_num, episode_dict))
            episodes[ep_num] = episode_dict

        season_air_date = season_details.get("air_date", "") or ""
        return season_number, {
            "originally_available": season_air_date,
            "episodes": episodes,
        }
    
    # Use ThreadPoolExecutor to process seasons in parallel
    max_workers = config.get("threads", {}).get("max_workers", 5)
    timeout_seconds = config.get("threads", {}).get("timeout", 300)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        season_futures = {executor.submit(process_season, s): s for s in details.get("seasons", [])}
        try:
            for future in as_completed(season_futures, timeout=timeout_seconds):
                season_number, season_data = future.result()
                if season_data:
                    seasons_data[season_number] = season_data
        except TimeoutError:
            logging.error(f"[Metadata Timeout] Season processing exceeded {timeout_seconds} seconds.")
        except Exception as e:
            logging.error(f"[Metadata Error] Season processing exception: {e}")

    # Fetch external IDs for mapping
    external_ids = details.get("external_ids", {})
    tvdb_id_for_mapping = external_ids.get("tvdb_id", "") if external_ids else ""
    imdb_id_for_mapping = external_ids.get("imdb_id", "") if external_ids else ""
    mapping_id = int(tvdb_id_for_mapping) if tvdb_id_for_mapping else imdb_id_for_mapping or ""

    # Build metadata entry for the show
    metadata_entry = {
        "match": {
            "title": title,
            "year": year,
            "mapping_id": mapping_id
        },
        **new_metadata,
        "seasons": seasons_data
    }

    # Prepare expected fields for completeness calculation
    show_expected_fields = [
        "sort_title", "original_title", "originally_available", "content_rating",
        "studio", "tagline", "summary", "country", "genre"
    ]
    season_expected_fields = ["originally_available"]
    episode_expected_fields = [
        "title", "sort_title", "originally_available", "runtime", "summary", "cast", "guest", "director", "writer"
    ]

    # Flatten all metadata for completeness calculation
    grand_metadata = {}
    for f in show_expected_fields:
        grand_metadata[f"show_{f}"] = new_metadata.get(f)
    for season_num, season_data in seasons_data.items():
        for f in season_expected_fields:
            grand_metadata[f"season{season_num}_{f}"] = season_data.get(f)
    for season_num, season_data in seasons_data.items():
        for ep_num, ep_data in season_data.get("episodes", {}).items():
            for f in episode_expected_fields:
                grand_metadata[f"season{season_num}_ep{ep_num}_{f}"] = ep_data.get(f)

    grand_expected_fields = list(grand_metadata.keys())
    grand_percent = log_metadata_completeness(
        "[TV Show Metadata]", full_title, grand_metadata, grand_expected_fields
    )

    # Smart update: check if anything changed
    if existing_yaml_data:
        existing_metadata = existing_yaml_data.get("metadata", {}).get(full_title, {})
        changes = smart_update_needed(existing_metadata, new_metadata)
        if not changes:
            logging.debug(f"[Metadata Update] No changes for {full_title}. Preserving existing metadata.")
            consolidated_metadata["metadata"][full_title] = existing_metadata
            return grand_percent

    if dry_run:
        logging.info(f"[Metadata Dry Run] Would build metadata for TV Show: {full_title}")
        return

    # Save metadata and update cache
    consolidated_metadata["metadata"][full_title] = metadata_entry

    # Use standardized cache key format
    cache_key = f"tv:{title}:{year}"
    update_tmdb_cache(cache_key, tmdb_id_int, title, year, "tv")
    logging.debug(f"[Metadata] TV metadata built and saved for {full_title} using TMDb ID {tmdb_id}.")
    return grand_percent
