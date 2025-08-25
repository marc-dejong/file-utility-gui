import re
from datetime import datetime

# Common date formats to test during parsing
COMMON_DATE_FORMATS = [
    "%Y-%m-%d", "%Y/%m/%d",
    "%m/%d/%Y", "%m-%d-%Y",
    "%d/%m/%Y", "%d-%m-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S.%f",
]

def denormalize_keys(record, field_indexes):
    """
    Given a record (list of strings) and a list of field indexes,
    return a list of denormalized key values.

    - Removes leading/trailing whitespace
    - Collapses multiple internal spaces to one
    - Converts ints/floats to standardized form
    - Parses and reformats date strings to YYYY-MM-DD
    """
    key = []

    for i in field_indexes:
        try:
            val = record[i]
        except IndexError:
            key.append("")
            continue

        if val is None:
            key.append("")
            continue

        val = val.strip()

        # Try int
        try:
            key.append(str(int(val)))
            continue
        except ValueError:
            pass

        # Try float
        try:
            key.append(str(float(val)))
            continue
        except ValueError:
            pass

        # Try date (normalize separators, remove time)
        val_normalized = re.sub(r"[.\-]", "/", val)
        parsed = False
        for fmt in COMMON_DATE_FORMATS:
            try:
                dt = datetime.strptime(val_normalized, fmt)
                key.append(dt.strftime("%Y-%m-%d"))  # Drop timestamp
                parsed = True
                break
            except ValueError:
                continue
        if parsed:
            continue

        # Clean string: collapse internal whitespace
        val_cleaned = re.sub(r'\s+', ' ', val)
        key.append(val_cleaned)

    return key
