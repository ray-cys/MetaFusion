import logging
from pathlib import Path

def get_plex_libraries(plex):
    """
    Retrieve a list of Plex libraries with their titles and types.

    Args:
        plex (PlexServer): The Plex server connection object.

    Returns:
        list: A list of dictionaries, each containing 'title' and 'type' of a library.
    """
    try:
        # Batch fetch all sections once
        sections = list(plex.library.sections())
        libraries = [{"title": section.title, "type": section.TYPE} for section in sections]
        logging.info(f"[Plex] Detected Libraries: {[lib['title'] for lib in libraries]}")
        return libraries
    except Exception as e:
        logging.error(f"[Plex] Failed to retrieve libraries: {e}")
        return []

def get_existing_plex_seasons_episodes(plex_item):
    """
    Get a mapping of seasons to episode numbers for a given Plex TV show item.

    Args:
        plex_item: The Plex TV show item.

    Returns:
        dict: A dictionary mapping season index to a list of episode numbers.
    """
    seasons_episodes = {}
    try:
        # Cache seasons locally to avoid repeated API calls
        seasons = list(plex_item.seasons())
        for season in seasons:
            # Cache episodes locally for each season
            episodes = list(season.episodes())
            episode_numbers = [ep.episodeNumber for ep in episodes]
            seasons_episodes[season.index] = episode_numbers
    except Exception as e:
        logging.warning(f"[Plex] Failed to get Plex seasons/episodes for {getattr(plex_item, 'title', 'Unknown')}: {e}")
    return seasons_episodes

def get_plex_movie_directory(item):
    """
    Get the directory name containing the movie file for a Plex movie item.

    Args:
        item: The Plex movie item.

    Returns:
        str: The name of the directory containing the movie file.
    """
    try:
        # Use iterParts only once and return on first valid part
        if hasattr(item, 'iterParts'):
            parts = list(item.iterParts())
            if parts:
                file_path = parts[0].file
                return Path(file_path).parent.name
    except Exception as e:
        logging.warning(f"[Plex] Failed to extract Plex directory name for {safe_title_year(item)}: {e}")
    # Fallback to title and year if extraction fails
    return f"{getattr(item, 'title', 'Unknown')} ({getattr(item, 'year', 'Unknown')})"

def get_plex_show_directory(item):
    """
    Get the directory name containing the show files for a Plex TV show item.

    Args:
        item: The Plex TV show item.

    Returns:
        str: The name of the directory containing the show's files.
    """
    try:
        # Cache episodes locally and return on first valid part
        if hasattr(item, 'episodes'):
            episodes = list(item.episodes())
            for episode in episodes:
                for media in getattr(episode, 'media', []):
                    for part in getattr(media, 'parts', []):
                        file_path = Path(part.file)
                        # Return the grandparent directory name (show folder)
                        return file_path.parent.parent.name
    except Exception as e:
        logging.warning(f"[Plex] Failed to extract Plex show directory for {safe_title_year(item)}: {e}")
    # Fallback to title and year if extraction fails
    return f"{getattr(item, 'title', 'Unknown')} ({getattr(item, 'year', 'Unknown')})"

def safe_title_year(item):
    """
    Safely get the title and year string for a Plex item.

    Args:
        item: The Plex item (movie or show).

    Returns:
        str: A string in the format "Title (Year)".
    """
    title = getattr(item, "title", None) or "Unknown Title"
    year = getattr(item, "year", None) or "Unknown Year"
    return f"{title} ({year})"