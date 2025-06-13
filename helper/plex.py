import asyncio
import logging
from pathlib import Path

def get_plex_libraries(plex):
    """
    Retrieve a list of Plex libraries with their titles and types.
    """
    try:
        sections = list(plex.library.sections())
        libraries = [{"title": section.title, "type": section.TYPE} for section in sections]
        logging.info(f"[Plex] Detected Libraries: {[lib['title'] for lib in libraries]}")
        return libraries
    except Exception as e:
        logging.error(f"[Plex] Failed to retrieve libraries: {e}")
        return []

async def get_existing_plex_seasons_episodes(plex_item, _season_cache=None, _episode_cache=None):
    """
    Get a mapping of seasons to episode numbers for a given Plex TV show item.
    Uses in-memory cache to avoid repeated API calls for the same item.
    Runs heavy PlexAPI calls in a thread to avoid blocking the event loop.
    """
    if _season_cache is None:
        _season_cache = {}
    if _episode_cache is None:
        _episode_cache = {}

    item_key = getattr(plex_item, 'ratingKey', id(plex_item))
    if item_key in _season_cache:
        seasons = _season_cache[item_key]
    else:
        seasons = await asyncio.to_thread(lambda: list(plex_item.seasons()))
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
    return seasons_episodes

async def get_plex_movie_directory(item, _movie_cache=None):
    """
    Get the directory name containing the movie file for a Plex movie item.
    Uses in-memory cache to avoid repeated API calls for the same item.
    Runs heavy PlexAPI calls in a thread to avoid blocking the event loop.
    """
    if _movie_cache is None:
        _movie_cache = {}
    try:
        item_key = getattr(item, 'ratingKey', id(item))
        if item_key in _movie_cache:
            parts = _movie_cache[item_key]
        else:
            parts = await asyncio.to_thread(lambda: list(item.iterParts())) if hasattr(item, 'iterParts') else []
            _movie_cache[item_key] = parts
        if parts:
            file_path = parts[0].file
            return Path(file_path).parent.name
    except Exception as e:
        logging.warning(f"[Plex] Failed to extract Plex directory name for {safe_title_year(item)}: {e}")
    # Fallback to title and year if extraction fails
    return f"{getattr(item, 'title', 'Unknown')} ({getattr(item, 'year', 'Unknown')})"

async def get_plex_show_directory(item, _episode_cache=None):
    """
    Get the directory name containing the show files for a Plex TV show item.
    Uses in-memory cache to avoid repeated API calls for the same item.
    Runs heavy PlexAPI calls in a thread to avoid blocking the event loop.
    """
    if _episode_cache is None:
        _episode_cache = {}

    try:
        if hasattr(item, 'episodes'):
            item_key = getattr(item, 'ratingKey', id(item))
            if item_key in _episode_cache:
                episodes = _episode_cache[item_key]
            else:
                episodes = await asyncio.to_thread(lambda: list(item.episodes()))
                _episode_cache[item_key] = episodes
            for episode in episodes:
                for media in getattr(episode, 'media', []):
                    for part in getattr(media, 'parts', []):
                        file_path = Path(part.file)
                        return file_path.parent.parent.name
    except Exception as e:
        logging.warning(f"[Plex] Failed to extract Plex show directory for {safe_title_year(item)}: {e}")
    return f"{getattr(item, 'title', 'Unknown')} ({getattr(item, 'year', 'Unknown')})"

def safe_title_year(item):
    """
    Safely get the title and year string for a Plex item.
    """
    title = getattr(item, "title", None) or "Unknown Title"
    year = getattr(item, "year", None) or "Unknown Year"
    return f"{title} ({year})"