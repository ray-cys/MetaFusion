import shutil, asyncio
from helper.logging import log_builder_event, log_asset_status
from helper.cache import meta_cache_async
from helper.plex import get_plex_country
from helper.tmdb import tmdb_api_request, tmdb_response_cache
from modules.utils import (
    smart_meta_update, get_meta_field, smart_asset_upgrade, asset_temp_path, get_best_poster, 
    get_best_background, download_poster, get_asset_path
)

async def build_movie(
    config, consolidated_metadata, feature_flags=None, existing_yaml_data=None, session=None, ignored_fields=None,
    existing_assets=None, library_name=None, meta=None, 
):
    result = {
        "poster": {"size": 0},
        "background": {"size": 0},
    }
        
    if not feature_flags or not feature_flags.get("metadata_basic", True):
        return
    if ignored_fields is None:
        ignored_fields = set()
    if existing_assets is None:
        existing_assets = set()
    title = meta.get("title", "Unknown") if meta else None
    year = meta.get("year", "Unknown") if meta else None
    cache_key = f"movie:{title}:{year}"
    movie_path = meta.get("movie_path") if meta else None
    tmdb_id = meta.get("tmdb_id") if meta else None
    full_title = f"{title} ({year})"
    imdb_id_for_mapping = ""
    mapping_id = ""

    if not tmdb_id:
        search_results = await tmdb_api_request(
            config,
            "search/movie",
            params={"query": title},
            session=session
        )
        if search_results and search_results.get("results"):
            movie_id = search_results["results"][0].get("id")
            if movie_id:
                external_ids = await tmdb_api_request(
                    config,
                    f"movie/{movie_id}/external_ids"
                )
                if external_ids:
                    imdb_id_for_mapping = external_ids.get("imdb_id", "")
                    tmdb_id = movie_id
        mapping_id = imdb_id_for_mapping or ""
        if not tmdb_id or not mapping_id:
            log_builder_event("builder_no_tmdb_id", media_type="Movie", id_type="IMDb", full_title=full_title)
            return
    else:
        mapping_id = int(tmdb_id)

    details_key = f"movie/{tmdb_id}"
    details = tmdb_response_cache.get(details_key)
    if not details:
        details = await tmdb_api_request(
            config,
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
            log_builder_event("builder_invalid_tmdb_id", media_type="Movie", full_title=full_title)
            return

    release_dates = get_meta_field(details, "results", [], path=["release_dates"])
    content_rating = next(
        (c.get("certification", "")
        for country in release_dates
        if country.get("iso_3166_1") == "US"
        for c in country.get("release_dates", []) if c.get("certification")), ""
    )

    genres = [g.get("name", "") for g in get_meta_field(details, "genres", [])]
    studio = ", ".join([c.get("name", "") for c in get_meta_field(details, "production_companies", []) if c.get("name")]) or ""
    release_date = get_meta_field(details, "release_date", "")

    production_countries = get_meta_field(details, "production_countries", [])
    country_codes = [c.get("iso_3166_1", "") for c in production_countries if c.get("iso_3166_1")]
    countries = [get_plex_country(code) for code in country_codes]

    originally_available = release_date or ""
    runtime = get_meta_field(details, "runtime", None)

    collection_info = get_meta_field(details, "belongs_to_collection", {})
    collection_id = get_meta_field(collection_info, "id", None)
    collection_name = get_meta_field(collection_info, "name", "")
    cleaned_collection = collection_name.removesuffix(" Collection")

    director_jobs = {"Director", "Co-Director", "Assistant Director"}
    writer_jobs = {"Writer", "Screenplay", "Story", "Creator", "Co-Writer", "Author", "Adaptation"}
    producer_jobs = {"Producer", "Executive Producer", "Associate Producer", "Co-Producer", "Line Producer", "Co-Executive Producer"}
    
    credits = get_meta_field(details, "credits", {})
    crew = get_meta_field(credits, "crew", [])
    cast = get_meta_field(credits, "cast", [])
    directors = [m.get("name", "") for m in crew if m.get("job") in director_jobs]
    writers = [m.get("name", "") for m in crew if m.get("job") in writer_jobs]
    producers = [m.get("name", "") for m in crew if m.get("job") in producer_jobs]
    top_cast = [c.get("name", "") for c in cast[:10]]

    basic_fields = [
        "sort_title", "original_title", "originally_available", "content_rating",
        "studio", "runtime", "tagline", "summary", "country.sync", "genre.sync"
    ]
    enhanced_fields = [
        "cast.sync", "director.sync", "writer.sync", "producer.sync", "collection.sync"
    ]
    fields_to_write = basic_fields + (enhanced_fields if feature_flags.get("metadata_enhanced", True) else [])

    new_metadata = {k: v for k, v in {
        "sort_title": title,
        "original_title": get_meta_field(details, "original_title", title),
        "originally_available": originally_available,
        "content_rating": content_rating,
        "studio": studio or "",
        "runtime": runtime,
        "tagline": get_meta_field(details, "tagline", ""),
        "summary": get_meta_field(details, "overview", ""),
        "country.sync": countries or [],
        "genre.sync": genres or [],
        "cast.sync": top_cast or [],
        "director.sync": directors or [],
        "writer.sync": writers or [],
        "producer.sync": producers or [],
        "collection.sync": cleaned_collection,
    }.items() if k in fields_to_write}

    expected_fields = [f for f in fields_to_write if f != "collection.sync"]
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
    percent = percent_filled

    metadata_changed = False
    changes = []
    if existing_yaml_data:
        existing_metadata = existing_yaml_data.get("metadata", {}).get(full_title, {})
        changes = smart_meta_update(existing_metadata, new_metadata)
        if not changes:
            log_builder_event("builder_no_metadata_changes", media_type="Movie", full_title=full_title, percent=percent)
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
    else:
        consolidated_metadata["metadata"][full_title] = {
            "match": {
                "title": title,
                "year": year,
                "mapping_id": mapping_id
            },
            **new_metadata
        }
        log_builder_event("builder_no_existing_metadata", media_type="Movie", full_title=full_title, tmdb_id=tmdb_id)
        metadata_changed = True
        changes = list(new_metadata.keys())
    
    if feature_flags.get("dry_run", False):
        log_builder_event("builder_dry_run_metadata", media_type="Movie", full_title=full_title)

    if metadata_changed:
        cache_key = f"movie:{title}:{year}"
        await meta_cache_async(cache_key, tmdb_id, title, year, "movie", collection_id=collection_id)
        log_builder_event(
            "builder_metadata_upgraded", media_type="Movie", full_title=full_title, 
            percent=percent, tmdb_id=tmdb_id, changes=changes
        )

    async def process_poster():
        poster_size = 0
        if not feature_flags or not feature_flags.get("poster", True):
            result["poster"]["size"] = poster_size
            return
        if not movie_path:
            log_builder_event("builder_no_asset_path", media_type="Movie", full_title=full_title, asset_type="poster", extra="")
            result["poster"]["size"] = poster_size
            return

        if feature_flags.get("dry_run", False):
            log_builder_event("builder_dry_run_asset", media_type="Movie", asset_type="poster", full_title=full_title)
            result["poster"]["size"] = poster_size
            return
        
        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        images = get_meta_field(details, "posters", [], path=["images"])
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_poster(config, images, preferred_language=preferred_language, fallback=fallback)
        if not best:
            log_builder_event("builder_no_suitable_asset", media_type="Movie", asset_type="poster", full_title=full_title, extra="")
            result["poster"]["size"] = poster_size
            return   

        asset_path = get_asset_path(config, meta, asset_type="poster")
        if asset_path is None:
            log_builder_event("builder_no_asset_path", media_type="Movie", full_title=full_title, asset_type="poster", extra="")
            result["poster"]["size"] = poster_size
            return

        temp_path = asset_temp_path(config, library_name)
        try:
            success, url, status, error = await download_poster(config, best["file_path"], temp_path, session=session)
            if not success:
                log_builder_event(
                    "builder_asset_download_failed", media_type="Movie", asset_type="poster",
                    full_title=full_title, url=url, status=status, error=error
                )
            if success and temp_path.exists():
                should_upgrade, status_code, context = smart_asset_upgrade(
                    config, asset_path, best, new_image_path=temp_path, asset_type="poster", cache_key=cache_key
                )
                if should_upgrade:
                    asset_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(temp_path, asset_path)
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    poster_size = asset_path.stat().st_size if asset_path.exists() else 0
                    await meta_cache_async(cache_key, tmdb_id, title, year, "movie", poster_average=best.get("vote_average", 0))
                    if status_code == "NO_EXISTING_ASSET":
                        log_builder_event(
                            "builder_downloading_asset", media_type="Movie", asset_type="poster",
                            full_title=full_title, filesize=poster_size
                        )
                    else:
                        log_builder_event(
                            "builder_asset_upgraded", media_type="Movie", asset_type="Poster",
                            full_title=full_title, status_code=status_code, context=context, filesize=poster_size
                        )
                    existing_assets.add(str(asset_path.resolve()))
                else:
                    poster_size = asset_path.stat().st_size if asset_path.exists() else 0
                    log_asset_status(
                        status_code, media_type="Movie", asset_type="poster", full_title=full_title,
                        filesize=poster_size, error=context.get("error") if context else None, extra="", season_number=None
                    )
                    if asset_path.exists():
                        existing_assets.add(str(asset_path.resolve()))
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        result["poster"]["size"] = poster_size

    async def process_background():
        background_size = 0
        if not feature_flags or not feature_flags.get("background", True):
            result["background"]["size"] = background_size
            return
        if not movie_path:
            log_builder_event("builder_no_asset_path", media_type="Movie", full_title=full_title, asset_type="background", extra="")
            result["background"]["size"] = background_size
            return

        if feature_flags.get("dry_run", False):
            log_builder_event("builder_dry_run_asset", media_type="Movie", asset_type="background", full_title=full_title)
            result["background"]["size"] = background_size
            return
    
        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        images = get_meta_field(details, "backdrops", [], path=["images"])
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_background(config, images, preferred_language=preferred_language, fallback=fallback)
        if not best:
            log_builder_event("builder_no_suitable_asset", media_type="Movie", asset_type="background", full_title=full_title, extra="")
            result["background"]["size"] = background_size
            return

        asset_path = get_asset_path(config, meta, asset_type="background")
        if asset_path is None:
            log_builder_event("builder_no_asset_path", media_type="Movie", full_title=full_title, asset_type="background", extra="")
            result["background"]["size"] = background_size
            return

        temp_path = asset_temp_path(config, library_name)
        try:
            success, url, status, error = await download_poster(config, best["file_path"], temp_path, session=session)
            if not success:
                log_builder_event(
                    "builder_asset_download_failed", media_type="Movie", asset_type="background",
                    full_title=full_title, url=url, status=status, error=error
                )
            if success and temp_path.exists():
                should_upgrade, status_code, context = smart_asset_upgrade(
                    config, asset_path, best, new_image_path=temp_path, asset_type="background", cache_key=cache_key
                )
                if should_upgrade:
                    asset_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(temp_path, asset_path)
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    background_size = asset_path.stat().st_size if asset_path.exists() else 0
                    await meta_cache_async(cache_key, tmdb_id, title, year, "movie", bg_average=best.get("vote_average", 0))
                    if status_code == "NO_EXISTING_ASSET":
                        log_builder_event(
                            "builder_downloading_asset", media_type="Movie", asset_type="background",
                            full_title=full_title, filesize=background_size
                        )
                    else:
                        log_builder_event(
                        "builder_asset_upgraded", media_type="Movie", asset_type="Background",
                        full_title=full_title, status_code=status_code, context=context, filesize=background_size
                    )
                    existing_assets.add(str(asset_path.resolve()))
                else:
                    background_size = asset_path.stat().st_size if asset_path.exists() else 0
                    log_asset_status(
                        status_code, media_type="Movie", asset_type="background", full_title=full_title,
                        filesize=background_size, error=context.get("error") if context else None, extra="", season_number=None
                    )
                    if asset_path.exists():
                        existing_assets.add(str(asset_path.resolve()))
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        result["background"]["size"] = background_size

    await asyncio.gather(
        process_poster(),
        process_background(),
    )
    
    return {
        "percent": percent,
        **result
    }

async def build_tv(
    config, consolidated_metadata, feature_flags=None, existing_yaml_data=None, session=None, ignored_fields=None,
    existing_assets=None, library_name=None, meta=None, 
):
    result = {
        "poster": {"size": 0},
        "background": {"size": 0},
        "season_poster": {"size": 0},
        "season_posters": {}, 
    }
    if not feature_flags or not feature_flags.get("metadata_basic", True):
        return 
    if ignored_fields is None:
        ignored_fields = set()
    if existing_assets is None:
        existing_assets = set()
    title = meta.get("title", "Unknown") if meta else None
    year = meta.get("year", "Unknown") if meta else None
    cache_key = f"tv:{title}:{year}"
    full_title = f"{title} ({year})"
    tmdb_id = meta.get("tmdb_id") if meta else None
    show_path = meta.get("show_path") if meta else None
    seasons_episodes = meta.get("seasons_episodes") if meta else None

    if not tmdb_id:
        search_results = await tmdb_api_request(
            config,
            "search/tv",
            params={"query": title},
            session=session
        )
        tvdb_id_for_mapping = ""
        if search_results and search_results.get("results"):
            tv_id = search_results["results"][0].get("id")
            if tv_id:
                external_ids = await tmdb_api_request(
                    config,
                    f"tv/{tv_id}/external_ids"
                )
                if external_ids:
                    tvdb_id_for_mapping = external_ids.get("tvdb_id", "")
                    tmdb_id = tv_id
        mapping_id = int(tvdb_id_for_mapping) if tvdb_id_for_mapping else ""
        if not tmdb_id or not mapping_id:
            log_builder_event("builder_no_tmdb_id", media_type="TV Show", id_type="TVDb", full_title=full_title)
            return {
                "percent": 0,
                "poster": {"size": 0},
                "season_poster": {"size": 0},
                "background": {"size": 0}
            }
    else:
        mapping_id = None 

    try:
        tmdb_id_int = int(tmdb_id)
    except (ValueError, TypeError):
        log_builder_event("builder_invalid_tmdb_id", media_type="TV Show", full_title=full_title)
        return {
            "percent": 0,
            "poster": {"size": 0},
            "season_poster": {"size": 0},
            "background": {"size": 0}
        }

    details_key = f"tv/{tmdb_id_int}"
    details = tmdb_response_cache.get(details_key)
    if not details:
        details = await tmdb_api_request(
            config,
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
            log_builder_event("builder_no_tmdb_id", media_type="TV Show", full_title=full_title)
            return {
                "percent": 0,
                "poster": {"size": 0},
                "season_poster": {"size": 0},
                "background": {"size": 0}
            }

    content_ratings = get_meta_field(details, "results", [], path=["content_ratings"])
    content_rating = next(
        (c.get("rating", "") for c in content_ratings if c.get("iso_3166_1") == "US"), ""
    )
    
    genres = [g.get("name", "") for g in get_meta_field(details, "genres", [])]
    studios = [n.get("name", "") for n in get_meta_field(details, "networks", []) if n.get("name")]
    studio = ", ".join(studios) if studios else ""
    originally_available = get_meta_field(details, "first_air_date", "") or ""
    country_codes = get_meta_field(details, "origin_country", [])
    countries = [get_plex_country(code) for code in country_codes]

    show_basic_fields = [
        "sort_title", "original_title", "originally_available", "content_rating",
        "studio", "summary", "country.sync", "genre.sync", "seasons"
    ]
    show_enhanced_fields = ["tagline"]
    show_fields_to_write = show_basic_fields + (show_enhanced_fields if feature_flags.get("metadata_enhanced", True) else [])
    
    new_metadata = {k: v for k, v in {
        "sort_title": title,
        "original_title": details.get("original_name", title),
        "originally_available": originally_available,
        "content_rating": content_rating,
        "studio": studio or "",
        "tagline": details.get("tagline", ""),
        "summary": details.get("overview", ""),
        "genre.sync": genres or [],
        "country.sync": countries or [],
    }.items() if k in show_fields_to_write and (k != "seasons")}

    seasons_data = {}
    async def process_season(season_info):
        season_number = season_info.get("season_number")
        if season_number == 0 or not seasons_episodes or season_number not in seasons_episodes:
            return season_number, None

        season_key = f"tv/{tmdb_id_int}/season/{season_number}"
        season_details = tmdb_response_cache.get(season_key)
        if not season_details:
            season_details = await tmdb_api_request(
                config,
                season_key,
                params={"append_to_response": "credits,images"},
                session=session
            )
            if season_details:
                tmdb_response_cache[season_key] = season_details
            else:
                log_builder_event(
                    "builder_no_tmdb_season_data", media_type="TV Shows",
                    season_number=season_number, full_title=full_title
                )
                return season_number, None

        show_credits = get_meta_field(details, "credits", {})
        show_crew = get_meta_field(show_credits, "crew", [])
        show_cast = get_meta_field(show_credits, "cast", [])
        
        season_credits = get_meta_field(season_details, "credits", {})
        season_crew = get_meta_field(season_credits, "crew", [])
        season_cast = get_meta_field(season_credits, "cast", [])

        ep_director_jobs = {"Director", "Co-Director", "Assistant Director"}
        ep_writer_jobs = {"Writer", "Screenplay", "Story", "Creator", "Co-Writer", "Author", "Adaptation"}

        episodes = {}
        for episode in get_meta_field(season_details, "episodes", []):
            ep_num = episode.get("episode_number")
            if not seasons_episodes or season_number not in seasons_episodes or ep_num not in seasons_episodes[season_number]:
                continue
            ep_crew = get_meta_field(episode, "crew", []) or season_crew or show_crew
            ep_credits = get_meta_field(episode, "credits", {})
            ep_cast = get_meta_field(ep_credits, "cast", []) or season_cast or show_cast
            ep_guest_stars = get_meta_field(ep_credits, "guest_stars", [])
            
            ep_directors = [m.get("name", "") for m in ep_crew if m.get("job") in ep_director_jobs]
            ep_writers = [m.get("name", "") for m in ep_crew if m.get("job") in ep_writer_jobs]
            ep_cast = [c.get("name", "") for c in ep_cast[:10]]
            ep_guest_stars = [g.get("name", "") for g in ep_guest_stars[:5]]
            ep_air_date = get_meta_field(episode, "air_date", "") or ""
            ep_runtime = get_meta_field(episode, "runtime", None)

            ep_basic_fields = ["sort_title", "original_title", "originally_available", "runtime", "summary"]
            ep_enhanced_fields = ["cast.sync", "guest.sync", "director.sync", "writer.sync"]
            ep_fields_to_write = ep_basic_fields + (ep_enhanced_fields if config["metadata"].get("run_enhanced", True) else [])

            episode_dict = {k: v for k, v in {
                "title": get_meta_field(episode, "name", ""),
                "sort_title": get_meta_field(episode, "name", ""),
                "originally_available": ep_air_date,
                "runtime": ep_runtime,
                "summary": get_meta_field(episode, "overview", ""),
                "cast.sync": ep_cast or [],
                "guest.sync": ep_guest_stars or [],
                "director.sync": ep_directors or [],
                "writer.sync": ep_writers or [],
            }.items() if k in ep_fields_to_write}
            episodes[ep_num] = episode_dict

        season_air_date = get_meta_field(season_details, "air_date", "") or ""
        return season_number, {
            "originally_available": season_air_date,
            "episodes": episodes,
            "season_details": season_details
        }

    season_infos = get_meta_field(details, "seasons", [])
    results = await asyncio.gather(*(process_season(s) for s in season_infos))
    for season_number, season_data in results:
        if season_data:
            seasons_data[season_number] = {k: v for k, v in season_data.items() if k != "season_details"}

    external_ids = get_meta_field(details, "external_ids", {})
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
    grand_percent = percent_filled

    metadata_changed = False
    changes = []
    if existing_yaml_data:
        existing_metadata = existing_yaml_data.get("metadata", {}).get(full_title, {})
        changes = smart_meta_update(existing_metadata, {**new_metadata, "seasons": seasons_data})
        if not changes:
            log_builder_event("builder_no_metadata_changes", media_type="TV Show", full_title=full_title, percent=grand_percent)
        else:
            consolidated_metadata["metadata"][full_title] = metadata_entry
            metadata_changed = True
    else:
        consolidated_metadata["metadata"][full_title] = metadata_entry
        log_builder_event("builder_no_existing_metadata", media_type="TV Show", full_title=full_title, tmdb_id=tmdb_id)
        metadata_changed = True
        changes = list(metadata_entry.keys())
    
    if feature_flags.get("dry_run", False):
        log_builder_event("builder_dry_run_metadata", media_type="TV Show", full_title=full_title)

    if metadata_changed:
        cache_key = f"tv:{title}:{year}"
        await meta_cache_async(cache_key, tmdb_id_int, title, year, "tv")
        log_builder_event(
            "builder_metadata_upgraded", media_type="TV Show",
            full_title=full_title, percent=grand_percent, tmdb_id=tmdb_id, changes=changes
        )

    async def process_tv_poster():
        poster_size = 0
        if not feature_flags or not feature_flags.get("poster", True):
            result["poster"]["size"] = poster_size
            return
        if not show_path:
            log_builder_event("builder_no_asset_path", media_type="TV Show", full_title=full_title, asset_type="poster", extra="")
            result["poster"]["size"] = poster_size
            return

        if feature_flags.get("dry_run", False):
            log_builder_event("builder_dry_run_asset", media_type="TV Show", asset_type="poster", full_title=full_title)
            result["poster"]["size"] = poster_size
            return
            
        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        images = get_meta_field(details, "posters", [], path=["images"])
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_poster(config, images, preferred_language=preferred_language, fallback=fallback)
        if not best:
            log_builder_event("builder_no_suitable_asset", media_type="TV Show", asset_type="poster", full_title=full_title, extra="")
            result["poster"]["size"] = poster_size
            return

        asset_path = get_asset_path(config, meta, asset_type="poster")
        if asset_path is None:
            log_builder_event("builder_no_asset_path", media_type="TV Show", full_title=full_title, asset_type="poster", extra="")
            result["poster"]["size"] = poster_size
            return

        temp_path = asset_temp_path(config, library_name)
        try:
            success, url, status, error = await download_poster(config, best["file_path"], temp_path, session=session)
            if not success:
                log_builder_event(
                    "builder_asset_download_failed", media_type="TV Show", asset_type="poster",
                    full_title=full_title, url=url, status=status, error=error
                )
            if success and temp_path.exists():
                should_upgrade, status_code, context = smart_asset_upgrade(
                    config, asset_path, best, new_image_path=temp_path, asset_type="poster", cache_key=cache_key,
                    season_number=season_number
                )
                if should_upgrade:
                    asset_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(temp_path, asset_path)
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    poster_size = asset_path.stat().st_size if asset_path.exists() else 0
                    await meta_cache_async(cache_key, tmdb_id_int, title, year, "tv", poster_average=best.get("vote_average", 0))
                    if status_code == "NO_EXISTING_ASSET":
                        log_builder_event(
                            "builder_downloading_asset", media_type="TV Show", asset_type="poster",
                            full_title=full_title, filesize=poster_size
                        )
                    else:
                        log_builder_event(
                            "builder_asset_upgraded", media_type="TV Show", asset_type="Poster",
                            full_title=full_title, status_code=status_code, context=context, filesize=poster_size
                        )
                    existing_assets.add(str(asset_path.resolve()))
                else:
                    poster_size = asset_path.stat().st_size if asset_path.exists() else 0
                    log_asset_status(
                        status_code, media_type="TV Show", asset_type="poster", full_title=full_title,
                        filesize=poster_size, error=context.get("error") if context else None, extra="", season_number=None
                    )
                    if asset_path.exists():
                        existing_assets.add(str(asset_path.resolve()))
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        result["poster"]["size"] = poster_size

    async def process_tv_background():
        background_size = 0
        if not config["assets"].get("run_background", True):
            result["background"]["size"] = background_size
            return
        if not show_path:
            log_builder_event("builder_no_asset_path", media_type="TV Show", full_title=full_title, asset_type="background", extra="")
            result["background"]["size"] = background_size
            return

        if feature_flags.get("dry_run", False):
            log_builder_event("builder_dry_run_asset", media_type="TV Show", asset_type="background", full_title=full_title)
            result["background"]["size"] = background_size
            return
            
        images = get_meta_field(details, "backdrops", [], path=["images"])
        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_background(config, images, preferred_language=preferred_language, fallback=fallback)
        if not best:
            log_builder_event("builder_no_suitable_asset", media_type="TV Show", asset_type="background", full_title=full_title, extra="")
            result["background"]["size"] = background_size
            return
    
        asset_path = get_asset_path(config, meta, asset_type="background")
        if asset_path is None:
            log_builder_event("builder_no_asset_path", media_type="TV Show", full_title=full_title, asset_type="background", extra="")
            result["background"]["size"] = background_size
            return
    
        temp_path = asset_temp_path(config, library_name)
        try:
            success, url, status, error = await download_poster(config, best["file_path"], temp_path, session=session)
            if not success:
                log_builder_event(
                    "builder_asset_download_failed", media_type="TV Show", asset_type="background",
                    full_title=full_title, url=url, status=status, error=error
                )
            if success and temp_path.exists():
                should_upgrade, status_code, context = smart_asset_upgrade(
                    config, asset_path, best, new_image_path=temp_path, asset_type="background", cache_key=cache_key,
                    season_number=season_number
                )
                if should_upgrade:
                    asset_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(temp_path, asset_path)
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    background_size = asset_path.stat().st_size if asset_path.exists() else 0
                    await meta_cache_async(cache_key, tmdb_id_int, title, year, "tv", bg_average=best.get("vote_average", 0))
                    if status_code == "NO_EXISTING_ASSET":
                        log_builder_event(
                            "builder_downloading_asset", media_type="TV Show", asset_type="background",
                            full_title=full_title, filesize=background_size
                        )
                    else:
                        log_builder_event(
                            "builder_asset_upgraded", media_type="TV Show", asset_type="Background",
                            full_title=full_title, status_code=status_code, context=context, filesize=background_size
                        )
                    existing_assets.add(str(asset_path.resolve()))
                else:
                    background_size = asset_path.stat().st_size if asset_path.exists() else 0
                    log_asset_status(
                        status_code, media_type="TV Show", asset_type="background", full_title=full_title,
                        filesize=background_size, error=context.get("error") if context else None, extra="", season_number=None
                    )
                    if asset_path.exists():
                        existing_assets.add(str(asset_path.resolve()))
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        result["background"]["size"] = background_size

    async def process_season_poster(season_info):
        season_poster_size = 0
        season_number = season_info.get("season_number")
        if not season_number or season_number == 0:
            return
        if not show_path:
            log_builder_event("builder_no_asset_path_season", media_type="TV Show", full_title=full_title, season_number=season_number)
            return

        if feature_flags.get("dry_run", False):
            log_builder_event("builder_dry_run_asset_season", media_type="TV Show", season_number=season_number, asset_type="poster", full_title=full_title)
            result["season_posters"][season_number] = season_poster_size
            return
        
        season_key = f"tv/{tmdb_id_int}/season/{season_number}"
        season_details = tmdb_response_cache.get(season_key)
        if not season_details:
            log_builder_event("builder_no_season_details", media_type="TV Show", full_title=full_title, season_number=season_number)
            return

        preferred_language = config["tmdb"].get("language", "en").split("-")[0]
        images = get_meta_field(season_details, "posters", [], path=["images"])
        fallback = config["tmdb"].get("fallback", [])
        best = get_best_poster(config, images, preferred_language=preferred_language, fallback=fallback)
        if not best:
            log_builder_event(
                "builder_no_suitable_asset_season", media_type="TV Show", asset_type="poster",
                full_title=full_title, season_number=season_number
            )
            return

        asset_path = get_asset_path(config, meta, asset_type="season", season_number=season_number)
        if asset_path is None:
            log_builder_event("builder_no_asset_path_season", media_type="TV Show", full_title=full_title, season_number=season_number)
            return

        temp_path = asset_temp_path(config, library_name)
        try:
            success, url, status, error = await download_poster(config, best["file_path"], temp_path, session=session)
            if not success:
                log_builder_event(
                    "builder_asset_download_failed_season", media_type="TV Show", asset_type="poster",
                    full_title=full_title, season_number=season_number, url=url, status=status, error=error
                )
            if success and temp_path.exists():
                should_upgrade, status_code, context = smart_asset_upgrade(
                    config, asset_path, best, new_image_path=temp_path, asset_type="season", cache_key=cache_key, 
                    season_number=season_number
                )
                if should_upgrade:
                    asset_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(temp_path, asset_path)
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    season_poster_size = asset_path.stat().st_size if asset_path.exists() else 0
                    await meta_cache_async(
                        cache_key, tmdb_id_int, title, year, "tv_season",
                        season_average=best.get("vote_average", 0), season_number=season_number
                    )
                    if status_code == "NO_EXISTING_ASSET":
                        log_builder_event(
                            "builder_downloading_asset_season", media_type="TV Show", asset_type="poster",
                            full_title=full_title, season_number=season_number, filesize=season_poster_size
                        )
                    else:
                        log_builder_event(
                            "builder_asset_upgraded_season", media_type="TV Show", asset_type="poster",
                            full_title=full_title, season_number=season_number, status_code=status_code, context=context,
                            filesize=season_poster_size
                        )
                    existing_assets.add(str(asset_path.resolve()))
                else:
                    season_poster_size = asset_path.stat().st_size if asset_path.exists() else 0
                    season_status_map = {
                        "NO_UPGRADE_NEEDED": "NO_UPGRADE_NEEDED_SEASON",
                        "NO_IMAGE_FOR_COMPARE": "NO_IMAGE_FOR_COMPARE_SEASON",
                        "ERROR_IMAGE_COMPARE": "ERROR_IMAGE_COMPARE_SEASON",
                    }
                    season_status_code = season_status_map.get(status_code, status_code)
                    log_asset_status(
                        season_status_code, media_type="TV Show", asset_type="poster", full_title=full_title,
                        filesize=season_poster_size, error=context.get("error") if context else None, extra="", season_number=season_number
                    )
                    if asset_path.exists():
                        existing_assets.add(str(asset_path.resolve()))
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        result["season_posters"][season_number] = season_poster_size
    
    season_poster_tasks = []
    if config["assets"].get("run_season", True):
        for season_info in season_infos:
            ...
            season_poster_tasks.append(process_season_poster(season_info))

    await asyncio.gather(
        process_tv_poster(),
        process_tv_background(),
        *season_poster_tasks
    )
    
    return {
        "percent": grand_percent,
        **result
    }