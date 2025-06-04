import logging

def log_metadata_completeness(level, name, metadata_dict, expected_fields, extra=""):
    """
    Log and calculate the percentage of expected metadata fields that are filled.
    """
    # Count how many expected fields are present and non-empty
    filled = sum(
        bool(metadata_dict.get(f)) and metadata_dict.get(f) != [] and metadata_dict.get(f) != ""
        for f in expected_fields
    )
    percent_filled = round((filled / len(expected_fields)) * 100)
    # Log the completeness percentage
    logging.info(
        f"[{level}] TMDb extracted ({filled}/{len(expected_fields)}) for {name}{extra}: {percent_filled:.0f}% "
    )
    return percent_filled

def check_metadata_completeness(
    metadata_dict,
    required_fields,
    ignored_fields=None,
    nested_fields=None
):
    """
    Checks completeness of metadata entries.
    """
    if ignored_fields is None:
        ignored_fields = set()
    if nested_fields is None:
        nested_fields = {}

    complete = 0
    incomplete = 0
    incomplete_keys = []

    for key, entry in metadata_dict.items():
        if not isinstance(entry, dict):
            incomplete += 1
            incomplete_keys.append(key)
            continue
        missing = [
            f for f in required_fields
            if f not in ignored_fields and (f not in entry or entry[f] in [None, "", []])
        ]
        # Recursively check nested fields (e.g., seasons, episodes)
        nested_incomplete = False
        for nested_field, (nested_req, nested_ign, nested_nested) in nested_fields.items():
            if nested_field in entry and isinstance(entry[nested_field], dict):
                c, ic, _ = check_metadata_completeness(
                    entry[nested_field], nested_req, nested_ign, nested_nested
                )
                if ic > 0:
                    nested_incomplete = True
        if not missing and not nested_incomplete:
            complete += 1
        else:
            incomplete += 1
            incomplete_keys.append(key)
    return complete, incomplete, incomplete_keys

def summarize_metadata_completeness(
    library_name,
    output_path,
    total_items,
    is_tv=False,
    ignored_fields=None,
    season_ignored=None,
    episode_ignored=None,
    required_fields=None,  # Optional: explicitly set required fields
):
    from ruamel.yaml import YAML
    """
    Summarizes metadata completeness count and lists of incomplete/missing items for a library.
    """
    if ignored_fields is None:
        ignored_fields = set()
    if season_ignored is None:
        season_ignored = set()
    if episode_ignored is None:
        episode_ignored = set()

    with open(output_path, "r", encoding="utf-8") as f:
        yaml = YAML()
        data = yaml.load(f) or {}
    metadata = data.get("metadata", {})

    # Dynamically determine required fields unless explicitly set
    if required_fields is not None:
        required = set(required_fields)
    else:
        all_fields = set()
        for entry in metadata.values():
            if isinstance(entry, dict):
                all_fields.update(entry.keys())
        required = all_fields - ignored_fields

    nested_fields = {}
    if is_tv:
        # Dynamically determine required fields for seasons/episodes unless ignored
        season_required = {"tmdb_id", "season_number"}
        episode_required = {"tmdb_id", "episode_number"}
        if season_ignored:
            season_required = season_required - set(season_ignored)
        if episode_ignored:
            episode_required = episode_required - set(episode_ignored)
        nested_fields = {
            "seasons": (season_required, set(season_ignored or []), {
                "episodes": (episode_required, set(episode_ignored or []), {})
            })
        }

    complete, incomplete, incomplete_keys = check_metadata_completeness(
        metadata, required, ignored_fields, nested_fields
    )

    percent_complete = round((complete / total_items) * 100, 2) if total_items else 0

    logging.info(
        f"[Metadata Summary] {library_name}: {complete}/{total_items} complete, {incomplete} incomplete metadata entries. ({percent_complete}%)"
    )
    if incomplete_keys:
        logging.debug(f"[Metadata Summary] {library_name}: Incomplete entries: {incomplete_keys}")

    return {
        "complete": complete,
        "incomplete": incomplete,
        "incomplete_keys": incomplete_keys,
        "total_items": total_items,
        "percent_complete": percent_complete,
    }
    
def human_readable_size(size, decimal_places=2):
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0 or unit == 'TB':
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024.0