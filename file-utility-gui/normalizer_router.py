def normalize_record(record, shared_data):
    """
    Basic normalization logic: strip strings, apply uppercase if flag is set.
    """
    import logging
    import pandas as pd

    logger = logging.getLogger("normalize_record_debug")
    logger.debug(f"[DEBUG] Record received: {type(record)} | {record}")

    try:
        # Strip whitespace from all string fields
        normalized = [cell.strip() if isinstance(cell, str) else cell for cell in record]

        # Optionally: convert to uppercase
        if shared_data.get("flags", {}).get("normalize_upper", False):
            normalized = [cell.upper() if isinstance(cell, str) else cell for cell in normalized]

        return pd.Series(normalized, index=record.index)

    except Exception as e:
        raise RuntimeError(f"Normalization error on record {record}: {e}")

