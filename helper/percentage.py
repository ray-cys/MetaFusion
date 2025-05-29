import logging

def log_metadata_completeness(level, name, metadata_dict, expected_fields, extra=""):
    """
    Log and calculate the percentage of expected metadata fields that are filled.

    This function checks how many of the expected fields in the metadata dictionary
    are present and non-empty, logs the percentage completeness, and returns it.

    Args:
        level (str): The log level or type (e.g., "INFO", "WARNING").
        name (str): The name of the item being logged (e.g., movie or show title).
        metadata_dict (dict): The metadata dictionary to check.
        expected_fields (list): List of field names expected to be present.
        extra (str, optional): Extra string to append to the log message.

    Returns:
        int: The percentage of expected fields that are filled.
    """
    # Count how many expected fields are present and non-empty
    filled = sum(
        bool(metadata_dict.get(f)) and metadata_dict.get(f) != [] and metadata_dict.get(f) != ""
        for f in expected_fields
    )
    percent_filled = round((filled / len(expected_fields)) * 100)
    # Log the completeness percentage
    logging.info(
        f"[{level}] {name}{extra}: {percent_filled:.0f}% TMDb metadata extracted ({filled}/{len(expected_fields)})"
    )
    return percent_filled