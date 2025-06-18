import asyncio
import logging
from pathlib import Path

_plex_cache = {}

async def plex_metadata(item, _season_cache=None, _episode_cache=None, _movie_cache=None):
    global _plex_cache
    if _season_cache is None:
        _season_cache = {}
    if _episode_cache is None:
        _episode_cache = {}
    if _movie_cache is None:
        _movie_cache = {}

    item_key = getattr(item, 'ratingKey', id(item))
    if item_key in _plex_cache:
        return _plex_cache[item_key]

    library_section = getattr(item, "librarySection", None)
    library_name = getattr(library_section, "title", None) or "Unknown"
    library_type = (getattr(library_section, "type", None) or getattr(item, "type", None) or "unknown").lower()
    if library_type == "show":
        library_type = "tv"

    title = getattr(item, "title", None)
    year = getattr(item, "year", None)
    title_year = f"{title} ({year})" if title and year else None
    ratingKey = getattr(item, "ratingKey", None)

    tmdb_id = imdb_id = tvdb_id = None
    for guid in getattr(item, "guids", []):
        if guid.id.startswith("tmdb://"):
            tmdb_id = guid.id.split("://")[1].split("?")[0]
        elif guid.id.startswith("imdb://"):
            imdb_id = guid.id.split("://")[1].split("?")[0]
        elif guid.id.startswith("tvdb://"):
            tvdb_id = guid.id.split("://")[1].split("?")[0]

    movie_path = None
    movie_dir = None
    if library_type == "movie" or hasattr(item, "iterParts"):
        try:
            if item_key in _movie_cache:
                parts = _movie_cache[item_key]
            else:
                parts = await asyncio.to_thread(lambda: list(item.iterParts())) if hasattr(item, 'iterParts') else []
                _movie_cache[item_key] = parts
            if parts:
                file_path = parts[0].file
                movie_path = Path(file_path).parent.name
                movie_dir = str(Path(file_path).parent)
        except Exception as e:
            logging.warning(f"[Plex] Failed to extract movie directory for {title} ({year}): {e}")

    show_path = None
    show_dir = None
    if library_type in ("show", "tv") or hasattr(item, "episodes"):
        try:
            if item_key in _episode_cache:
                episodes = _episode_cache[item_key]
            else:
                episodes = await asyncio.to_thread(lambda: list(item.episodes())) if hasattr(item, 'episodes') else []
                _episode_cache[item_key] = episodes
            found = False
            for episode in episodes:
                for media in getattr(episode, 'media', []):
                    for part in getattr(media, 'parts', []):
                        file_path = Path(part.file)
                        show_path = file_path.parent.parent.name
                        show_dir = str(file_path.parent.parent)
                        found = True
                        break
                    if found:
                        break
                if found:
                    break
        except Exception as e:
            logging.warning(f"[Plex] Failed to extract show directory for {title} ({year}): {e}")

    seasons_episodes = None
    if library_type in ("show", "tv") or hasattr(item, "seasons"):
        try:
            if item_key in _season_cache:
                seasons = _season_cache[item_key]
            else:
                seasons = await asyncio.to_thread(lambda: list(item.seasons())) if hasattr(item, 'seasons') else []
                _season_cache[item_key] = seasons

            seasons_episodes = {}
            for season in seasons:
                season_key = getattr(season, 'ratingKey', id(season))
                if season_key in _episode_cache:
                    episodes = _episode_cache[season_key]
                else:
                    episodes = await asyncio.to_thread(lambda: list(season.episodes()))
                    _episode_cache[season_key] = episodes
                episode_numbers = [ep.episodeNumber for ep in episodes]
                seasons_episodes[season.index] = episode_numbers
        except Exception as e:
            logging.warning(f"[Plex] Failed to extract seasons/episodes for {title} ({year}): {e}")

    result = {
        "library_name": library_name,
        "library_type": library_type,
        "title": title,
        "year": year,
        "title_year": title_year,
        "ratingKey": ratingKey,
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "tvdb_id": tvdb_id,
        "movie_path": movie_path,
        "movie_dir": movie_dir,
        "show_path": show_path,
        "show_dir": show_dir,
        "seasons_episodes": seasons_episodes,
    }
    _plex_cache[item_key] = result
    return result
