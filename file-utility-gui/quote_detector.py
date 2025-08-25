# quote_detector.py

def detect_field_quote_pattern(file_path, delimiter=";", encoding="utf-8"):
    """
    Detects which fields in the first data row are quoted.
    Returns a list of booleans (True if field was quoted, False otherwise).
    """
    quote_map = []

    try:
        with open(file_path, "r", encoding=encoding, newline="", errors="replace") as f:
            lines = f.readlines()

        if len(lines) < 2:
            return quote_map  # Not enough lines to detect quoting

        data_line = lines[1].strip()  # First data row (after header)

        raw_fields = data_line.split(delimiter)

        for raw in raw_fields:
            raw = raw.strip()
            if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
                quote_map.append(True)
            else:
                quote_map.append(False)

        return quote_map

    except Exception as e:
        print(f"[ERROR] Failed to detect quote pattern: {e}")
        return []
