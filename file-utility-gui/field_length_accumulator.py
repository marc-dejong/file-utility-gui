"""
field_length_accumulator.py

This module provides utilities for scanning input records (row-by-row) to determine
the maximum field lengths and decimal precision needed for normalization.

It is designed for use in streaming or memory-constrained environments where
the full dataset cannot be loaded at once.

Functions:
- init_length_tracker(num_fields): Initializes tracking structure.
- update_length_tracker(tracker, row_values, type_list): Updates tracker with one row's data.
- finalize_length_tracker(tracker, type_list): Produces final length and precision lists.
"""

def init_length_tracker(num_fields):
    """
    Initializes an empty tracker for max length and precision per field.

    Args:
        num_fields (int): Number of fields to track.

    Returns:
        list[dict]: A list of tracking dictionaries, one per field.
    """
    return [{"max_length": 0, "max_precision": 0} for _ in range(num_fields)]


def update_length_tracker(tracker, row_values, type_list):
    """
    Updates the tracker with values from a single row.

    Args:
        tracker (list[dict]): Tracker list returned from init_length_tracker()
        row_values (list[str|float|int]): One row of raw values
        type_list (list[str]): Data types for each field ('int', 'float', 'str')
    """
    for i, raw_val in enumerate(row_values):
        dtype = type_list[i]
        try:
            val_str = str(raw_val).replace("$", "").replace(",", "").replace("(", "-").replace(")", "").strip()

            if dtype == "int":
                val = str(int(float(val_str)))
                tracker[i]["max_length"] = max(tracker[i]["max_length"], len(val))

            elif dtype == "float":
                val = str(float(val_str))
                if "." in val:
                    precision = len(val.split(".")[1])
                else:
                    precision = 0
                tracker[i]["max_length"] = max(tracker[i]["max_length"], len(val))
                tracker[i]["max_precision"] = max(tracker[i]["max_precision"], precision)

            elif dtype == "str":
                tracker[i]["max_length"] = max(tracker[i]["max_length"], len(val_str))

        except Exception:
            continue  # Skip bad data silently for now


def finalize_length_tracker(tracker, type_list):
    """
    Converts tracker into length_list and precision_list for normalization.

    Args:
        tracker (list[dict]): Final accumulated tracker
        type_list (list[str]): Data types

    Returns:
        (list[int], list[int]): Tuple of (length_list, precision_list)
    """
    length_list = []
    precision_list = []

    for i, entry in enumerate(tracker):
        length_list.append(entry["max_length"])
        precision_list.append(entry["max_precision"] if type_list[i] == "float" else 0)

    return length_list, precision_list
