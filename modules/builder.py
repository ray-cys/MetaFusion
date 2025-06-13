import logging
import shutil
import asyncio
from pathlib import Path
from helper.config import load_config
from helper.tmdb import (
    tmdb_api_request, resolve_tmdb_id, update_meta_cache, tmdb_response_cache,
    download_poster
)
from helper.plex import (
    get_existing_plex_seasons_episodes, get_plex_movie_directory, get_plex_show_directory, safe_title_year
)
from modules.utils import (
    should_upgrade, smart_meta_update, generate_temp_path, get_best_poster, get_best_background
)
from helper.logging import human_readable_size

config = load_config()

async def build_movie(
    plex_item,
    consolidated_metadata,
    dry_run=False,
    existing_yaml_data=None,
    session=None,
    ignored_fields=None,
    existing_assets=None,
    library_name=None,
    movie_cache=None
):
    import pycountry
    """
    Build and consolidate metadata for a movie Plex item using TMDb data.
    Always checks and processes assets (poster/background), even if metadata is unchanged.
    Returns a dict with percent and asset sizes.
    """
    if not config.get("process_metadata", True):
        return

    if ignored_fields is None:
        ignored_fields = set()
    if existing_assets is None:
        existing_assets = set()
    title = getattr(plex_item, "title", "Unknown")
    year = getattr(plex_item, "year", "Unknown")
    full_title = f"{title} ({year})"
    tmdb_id = await resolve_tmdb_id(plex_item, title, year, "movie", session=session)
    imdb_id_for_mapping = ""
    mapping_id = ""

    # If no TMDb ID, try searching by title
    if not tmdb_id:
        search_results = await tmdb_api_request(
            "search/movie",
            params={"query": title},
            session=session
        )
        if search_results and search_results.get("results"):
            movie_id = search_results["results"][0].get("id")
            if movie_id:
                external_ids = await tmdb_api_request(f"movie/{movie_id}/external_ids")
                if external_ids:
                    imdb_id_for_mapping = external_ids.get("imdb_id", "")
                    tmdb_id = movie_id
        mapping_id = imdb_id_for_mapping or ""
        if not tmdb_id or not mapping_id:
            logging.warning(f"[Metadata] No TMDb or IMDb ID found for {full_title}. Skipping...")
            return
    else:
        mapping_id = int(tmdb_id)

    # Try to get movie details from cache, else fetch from TMDb
    details_key = f"movie/{tmdb_id}"
    details = tmdb_response_cache.get(details_key)
    if not details:
        details = await tmdb_api_request(
            details_key,
            params={
                "append_to_response": "credits,release_dates,external_ids,images",
                "language": config.get("tmdb", {}).get("language", "en"),
                "region": config.get("tmdb", {}).get("region", "US")
            },
            session=session
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

    basic_fields = [
        "sort_title", "original_title", "originally_available", "content_rating",
        "studio", "runtime", "tagline", "summary", "country", "genre"
    ]
    enhanced_fields = [
        "cast", "director", "writer", "producer", "collection"
    ]
    fields_to_write = basic_fields + (enhanced_fields if config.get("enhanced_metadata", True) else [])

    # Build metadata dictionary
    new_metadata = {k: v for k, v in {
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
    }.items() if k in fields_to_write}

    # Log completeness of metadata
    expected_fields = [f for f in fields_to_write if f != "collection"]
    if ignored_fields is None:
        ignored_fields = set()
    filtered_fields = [f for f in expected_fields if f not in ignored_fields]
    if not filtered_fields:
        percent_filled = 100
        filled = 0
    else:
        filled = sum(
            bool(new_metadata.get(f)) and new_metadata.get(f) != [] and new_metadata.get(f) != ""
            for f in filtered_fields
        )
        percent_filled = round((filled / len(filtered_fields)) * 100)
    logging.debug(
        f"[Movie] TMDb extracted ({filled}/{len(filtered_fields)}) for {full_title}: {percent_filled:.0f}% "
    )
    percent = percent_filled

    # Smart update: check if anything changed
    metadata_changed = False
    if existing_yaml_data:
        existing_metadata = existing_yaml_data.get("metadata", {}).get(full_title, {})
        changes = smart_meta_update(existing_metadata, new_metadata)
        if not changes:
            logging.info(f"[Metadata] No changes for {full_title} ({percent}%). Preserving existing metadata.")
            consolidated_metadata["metadata"][full_title] = existing_metadata
        else:
            logging.info(f"[Metadata] Fields changed for {full_title} ({percent}%). Updating metadata.")
            consolidated_metadata["metadata"][full_title] = {
                "match": {
                    "title": title,
                    "year": year,
                    "mapping_id": mapping_id
                },
                **new_metadata
            }
            metadata_changed = True
    else:
        consolidated_metadata["metadata"][full_title] = {
            "match": {
                "title": title,
                "year": year,
                "mapping_id": mapping_id
            },
            **new_metadata
        }
        metadata_changed = True

    if dry_run:
        logging.info(f"[Dry Run] Would build metadata for Movie: {full_title}")
        if config.get("process_posters", True):
            logging.info(f"[Dry Run] Would process poster for {full_title}")
        if config.get("process_backgrounds", True):
            logging.info(f"[Dry Run] Would process background for {full_title}")
        return {"percent": percent, "poster": {"size": 0}, "background": {"size": 0}}

    if metadata_changed:
        cache_key = f"movie:{title}:{year}"
        update_meta_cache(cache_key, tmdb_id, title, year, "movie")
        logging.info(f"[Metadata] Movie metadata built and saved for {full_title} ({percent}%) using TMDb ID {tmdb_id}.")

    poster_size = 0
    background_size = 0
    cache_key = f"movie:{title}:{year}"

    # Movie poster assets downloads
    if config.get("process_posters", True):
        parent_dir = await get_plex_movie_directory(plex_item, _movie_cache=movie_cache)
        asset_path = Path(config["assets_path"]) / library_name / parent_dir / "poster.jpg"
        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        images = details.get("images", {}).get("posters", [])
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)
        if best:
            temp_path = generate_temp_path(library_name)
            try:
                if await download_poster(best["file_path"], temp_path, plex_item, session=session) and temp_path.exists():
                    poster_size = temp_path.stat().st_size
                    if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=plex_item):
                        asset_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(temp_path, asset_path)
                        logging.info(f"[Assets] Poster upgraded for {full_title}. Filesize: {human_readable_size(poster_size)}")
                        update_meta_cache(cache_key, tmdb_id, title, year, "movie", poster_average=best.get("vote_average"))
                    else:
                        temp_path.unlink(missing_ok=True)
                        logging.info(f"[Assets] No poster upgrade needed for {full_title}")
                else:
                    logging.warning(f"[Assets] Poster download failed for {full_title}, skipping...")
            finally:
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
            existing_assets.add(str(asset_path.resolve()))
        else:
            logging.info(f"[Assets] No suitable poster found for {full_title}. Skipping...")

    # Movie background assets downloads
    if config.get("process_backgrounds", True):
        parent_dir = await get_plex_movie_directory(plex_item, _movie_cache=movie_cache)
        asset_path = Path(config["assets_path"]) / library_name / parent_dir / "fanart.jpg"
        images = details.get("images", {}).get("backdrops", [])
        best = get_best_background(images)
        if best:
            temp_path = generate_temp_path(library_name)
            try:
                if await download_poster(best["file_path"], temp_path, plex_item, session=session) and temp_path.exists():
                    background_size = temp_path.stat().st_size
                    if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=plex_item):
                        asset_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(temp_path, asset_path)
                        logging.info(f"[Assets] Background upgraded for {full_title}. Filesize: {human_readable_size(background_size)}")
                        update_meta_cache(cache_key, tmdb_id, title, year, "movie", bg_average=best.get("vote_average"))
                    else:
                        temp_path.unlink(missing_ok=True)
                        logging.info(f"[Assets] No background upgrade needed for {full_title}")
                else:
                    logging.warning(f"[Assets] Background download failed for {full_title}, skipping...")
            finally:
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
            existing_assets.add(str(asset_path.resolve()))
        else:
            logging.info(f"[Assets] No suitable background found for {full_title}. Skipping...")

    return {
        "percent": percent,
        "poster": {"size": poster_size},
        "background": {"size": background_size}
    }

async def build_tv(
    plex_item,
    consolidated_metadata,
    dry_run=False,
    existing_yaml_data=None,
    season_cache=None,
    episode_cache=None,
    session=None,
    ignored_fields=None,
    existing_assets=None,
    library_name=None
):
    import pycountry
    """
    Build and consolidate metadata for a TV show Plex item using TMDb data.
    Also downloads poster, season posters, and background if enabled in config.
    Returns a dict with percent and asset sizes.
    """
    if not config.get("process_metadata", True):
        return

    if ignored_fields is None:
        ignored_fields = set()
    if existing_assets is None:
        existing_assets = set()
    title = getattr(plex_item, "title", "Unknown")
    year = getattr(plex_item, "year", "Unknown")
    full_title = f"{title} ({year})"
    tmdb_id = await resolve_tmdb_id(plex_item, title, year, "tv", session=session)

    # If no TMDb ID, try searching by title
    if not tmdb_id:
        search_results = await tmdb_api_request(
            "search/tv",
            params={"query": title},
            session=session
        )
        tvdb_id_for_mapping = ""
        if search_results and search_results.get("results"):
            tv_id = search_results["results"][0].get("id")
            if tv_id:
                external_ids = await tmdb_api_request(f"tv/{tv_id}/external_ids")
                if external_ids:
                    tvdb_id_for_mapping = external_ids.get("tvdb_id", "")
                    tmdb_id = tv_id
        mapping_id = int(tvdb_id_for_mapping) if tvdb_id_for_mapping else ""
        if not tmdb_id or not mapping_id:
            logging.warning(f"[Metadata] No TMDb or TVDb ID found for {full_title}. Skipping...")
            return
    else:
        mapping_id = None 

    try:
        tmdb_id_int = int(tmdb_id)
    except (ValueError, TypeError):
        logging.warning(f"[Metadata] Invalid TMDb ID format for {full_title}. Skipping...")
        return

    # Try to get TV show details from cache, else fetch from TMDb
    details_key = f"tv/{tmdb_id_int}"
    details = tmdb_response_cache.get(details_key)
    if not details:
        details = await tmdb_api_request(
            details_key,
            params={
                "append_to_response": "credits,keywords,content_ratings,external_ids,images",
                "language": config.get("tmdb", {}).get("language", "en"),
                "region": config.get("tmdb", {}).get("region", "US")
            },
            session=session
        )
        if details:
            tmdb_response_cache[details_key] = details
        else:
            logging.warning(f"[Metadata] No TMDb data found for {full_title}. Skipping...")
            return

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

    show_basic_fields = [
        "sort_title", "original_title", "originally_available", "content_rating",
        "studio", "summary", "country", "genre", "seasons"
    ]
    show_enhanced_fields = ["tagline"]
    show_fields_to_write = show_basic_fields + (show_enhanced_fields if config.get("enhanced_metadata", True) else [])

    # Build metadata dictionary
    new_metadata = {k: v for k, v in {
        "sort_title": title,
        "original_title": details.get("original_name", title),
        "originally_available": originally_available,
        "content_rating": content_rating,
        "studio": studio or "",
        "tagline": details.get("tagline", ""),
        "summary": details.get("overview", ""),
        "genre": genres or [],
        "country": countries or [],
    }.items() if k in show_fields_to_write and (k != "seasons")}

    existing_seasons_episodes = await get_existing_plex_seasons_episodes(
        plex_item,
        _season_cache=season_cache,
        _episode_cache=episode_cache
    )
    seasons_data = {}

    async def process_season(season_info):
        season_number = season_info.get("season_number")
        if season_number == 0 or season_number not in existing_seasons_episodes:
            return season_number, None

        season_key = f"tv/{tmdb_id_int}/season/{season_number}"
        season_details = tmdb_response_cache.get(season_key)
        if not season_details:
            season_details = await tmdb_api_request(
                season_key,
                params={"append_to_response": "credits,images"},
                session=session
            )
            if season_details:
                tmdb_response_cache[season_key] = season_details
            else:
                logging.warning(f"[Metadata] No TMDb data found for Season {season_number} of {full_title}. Skipping...")
                return season_number, None

        show_crew = details.get("credits", {}).get("crew", []) or []
        show_cast = details.get("credits", {}).get("cast", []) or []
        season_crew = season_details.get("credits", {}).get("crew", []) or []
        season_cast = season_details.get("credits", {}).get("cast", []) or []

        ep_director_jobs = {"Director", "Co-Director", "Assistant Director"}
        ep_writer_jobs = {"Writer", "Screenplay", "Story", "Creator", "Co-Writer", "Author", "Adaptation"}

        episodes = {}
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

            ep_basic_fields = ["sort_title", "original_title", "originally_available", "runtime", "summary"]
            ep_enhanced_fields = ["cast", "guest", "director", "writer"]
            ep_fields_to_write = ep_basic_fields + (ep_enhanced_fields if config.get("enhanced_metadata", True) else [])

            episode_dict = {k: v for k, v in {
                "title": episode.get("name", ""),
                "sort_title": episode.get("name", ""),
                "originally_available": ep_air_date,
                "runtime": ep_runtime,
                "summary": episode.get("overview", ""),
                "cast": ep_cast or [],
                "guest": ep_guest_stars or [],
                "director": ep_directors or [],
                "writer": ep_writers or [],
            }.items() if k in ep_fields_to_write}
            episodes[ep_num] = episode_dict

        season_air_date = season_details.get("air_date", "") or ""
        return season_number, {
            "originally_available": season_air_date,
            "episodes": episodes,
            "season_details": season_details
        }

    season_infos = details.get("seasons", [])
    results = await asyncio.gather(*(process_season(s) for s in season_infos))
    for season_number, season_data in results:
        if season_data:
            seasons_data[season_number] = {k: v for k, v in season_data.items() if k != "season_details"}

    external_ids = details.get("external_ids", {})
    if mapping_id is None:
        tvdb_id_for_mapping = external_ids.get("tvdb_id", "") if external_ids else ""
        mapping_id = int(tvdb_id_for_mapping) if tvdb_id_for_mapping else ""

    metadata_entry = {
        "match": {
            "title": title,
            "year": year,
            "mapping_id": mapping_id
        },
        **new_metadata,
        "seasons": seasons_data
    }

    # Log completeness of metadata
    expected_fields = [f for f in show_fields_to_write if f != "seasons"]
    if ignored_fields is None:
        ignored_fields = set()
    filtered_fields = [f for f in expected_fields if f not in ignored_fields]
    if not filtered_fields:
        percent_filled = 100
        filled = 0
    else:
        filled = sum(
            bool(new_metadata.get(f)) and new_metadata.get(f) != [] and new_metadata.get(f) != ""
            for f in filtered_fields
        )
        percent_filled = round((filled / len(filtered_fields)) * 100)
    logging.debug(
        f"[TV Show] TMDb extracted ({filled}/{len(filtered_fields)}) for {full_title}: {percent_filled:.0f}% "
    )
    grand_percent = percent_filled

    # Smart update: check if anything changed
    metadata_changed = False
    if existing_yaml_data:
        existing_metadata = existing_yaml_data.get("metadata", {}).get(full_title, {})
        changes = smart_meta_update(existing_metadata, {**new_metadata, "seasons": seasons_data})
        if not changes:
            logging.info(f"[Metadata] No changes for {full_title} ({grand_percent}%). Preserving existing metadata.")
            consolidated_metadata["metadata"][full_title] = existing_metadata
        else:
            logging.info(f"[Metadata] Fields changed for {full_title} ({grand_percent}%). Updating metadata.")
            consolidated_metadata["metadata"][full_title] = metadata_entry
            metadata_changed = True
    else:
        consolidated_metadata["metadata"][full_title] = metadata_entry
        logging.info(f"[Metadata] No existing metadata for {full_title}. Creating new entry.")
        metadata_changed = True

    if dry_run:
        logging.info(f"[Dry Run] Would build metadata for TV Show: {full_title}")
        if config.get("process_posters", True):
            logging.info(f"[Dry Run] Would process poster for {full_title}")
        if config.get("process_season_posters", True):
            for season_info in season_infos:
                season_number = season_info.get("season_number")
                if season_number and season_number != 0:
                    logging.info(f"[Dry Run] Would process season poster for {full_title} Season {season_number}")
        if config.get("process_backgrounds", True):
            logging.info(f"[Dry Run] Would process background for {full_title}")
        return {"percent": grand_percent, "poster": {"size": 0}, "season_poster": {"size": 0}, "background": {"size": 0}}

    if metadata_changed:
        cache_key = f"tv:{title}:{year}"
        update_meta_cache(cache_key, tmdb_id_int, title, year, "tv")
        logging.info(f"[Metadata] TV metadata built and saved for {full_title} ({grand_percent}%) using TMDb ID {tmdb_id}.")
    
    poster_size = 0
    background_size = 0
    season_poster_size = 0
    cache_key = f"tv:{title}:{year}"
    
    # TV Show poster assets downloads
    if config.get("process_posters", True):
        parent_dir = await get_plex_show_directory(plex_item, _episode_cache=episode_cache)
        asset_path = Path(config["assets_path"]) / library_name / parent_dir / "poster.jpg"
        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        images = details.get("images", {}).get("posters", [])
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)
        if best:
            temp_path = generate_temp_path(library_name)
            try:
                if await download_poster(best["file_path"], temp_path, plex_item, session=session) and temp_path.exists():
                    poster_size = temp_path.stat().st_size
                    if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=plex_item):
                        asset_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(temp_path, asset_path)
                        logging.info(f"[Assets] Poster upgraded for {full_title}. Filesize: {human_readable_size(poster_size)}")
                        update_meta_cache(cache_key, tmdb_id, title, year, "tv", poster_average=best.get("vote_average"))
                    else:
                        temp_path.unlink(missing_ok=True)
                        logging.info(f"[Assets] No poster upgrade needed for {full_title}")
                else:
                    logging.warning(f"[Assets] Poster download failed for {full_title}, skipping...")
            finally:
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
            existing_assets.add(str(asset_path.resolve()))
        else:
            logging.info(f"[Assets] No suitable poster found for {full_title}. Skipping...)")
    
    # TV Show season posters assets downloads
    if config.get("process_season_posters", True):
        for season_info in season_infos:
            season_number = season_info.get("season_number")
            if not season_number or season_number == 0:
                continue
            parent_dir = await get_plex_show_directory(plex_item, _episode_cache=episode_cache)
            asset_path = Path(config["assets_path"]) / library_name / parent_dir / f"Season{season_number:02}.jpg"
            season_key = f"tv/{tmdb_id_int}/season/{season_number}"
            season_details = tmdb_response_cache.get(season_key)
            if not season_details:
                logging.info(f"[Assets] No season details for {full_title} Season {season_number}, skipping poster.")
                continue
            preferred_language = config["tmdb"].get("language", "en").split("-")[0]
            images = season_details.get("images", {}).get("posters", [])
            fallback = config["tmdb"].get("fallback", [])
            best = get_best_poster(images, preferred_language=preferred_language, fallback=fallback)
            if best:
                temp_path = generate_temp_path(library_name)
                try:
                    if await download_poster(best["file_path"], temp_path, plex_item, session=session) and temp_path.exists():
                        season_poster_size += temp_path.stat().st_size
                        season_cache_key = f"tv:{title}:{year}:season{season_number}"
                        if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=season_cache_key, item=plex_item, season_number=season_number):
                            asset_path.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(temp_path, asset_path)
                            logging.info(f"[Assets] Season poster upgraded for {full_title} Season {season_number}. Filesize: {human_readable_size(temp_path.stat().st_size)}")
                            update_meta_cache(season_cache_key, tmdb_id, title, year, "tv", poster_average=best.get("vote_average"))
                        else:
                            temp_path.unlink(missing_ok=True)
                            logging.info(f"[Assets] No season poster upgrade needed for {full_title} Season {season_number}")
                    else:
                        logging.warning(f"[Assets] Season poster download failed for {full_title} Season {season_number}, skipping...")
                finally:
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                existing_assets.add(str(asset_path.resolve()))
            else:
                logging.info(f"[Assets] No suitable season poster found for {full_title} Season {season_number}. Skipping...)")
    
    # TV Show background assets downloads
    if config.get("process_backgrounds", True):
        parent_dir = await get_plex_show_directory(plex_item, _episode_cache=episode_cache)
        asset_path = Path(config["assets_path"]) / library_name / parent_dir / "fanart.jpg"
        images = details.get("images", {}).get("backdrops", [])
        best = get_best_background(images)
        if best:
            temp_path = generate_temp_path(library_name)
            try:
                if await download_poster(best["file_path"], temp_path, plex_item, session=session) and temp_path.exists():
                    background_size = temp_path.stat().st_size
                    if should_upgrade(asset_path, best, new_image_path=temp_path, cache_key=cache_key, item=plex_item):
                        asset_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(temp_path, asset_path)
                        logging.info(f"[Assets] Background upgraded for {full_title}. Filesize: {human_readable_size(background_size)}")
                        update_meta_cache(cache_key, tmdb_id, title, year, "tv", bg_average=best.get("vote_average"))
                    else:
                        temp_path.unlink(missing_ok=True)
                        logging.info(f"[Assets] No background upgrade needed for {full_title}")
                else:
                    logging.warning(f"[Assets] Background download failed for {full_title}, skipping...")
            finally:
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
            existing_assets.add(str(asset_path.resolve()))
        else:
            logging.info(f"[Assets] No suitable background found for {full_title}. Skipping...)")
    
    return {
        "percent": grand_percent,
        "poster": {"size": poster_size},
        "season_poster": {"size": season_poster_size},
        "background": {"size": background_size}
    }