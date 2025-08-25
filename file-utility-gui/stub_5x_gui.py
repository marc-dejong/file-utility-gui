from stub_loader import load_stub_lookup_table_v2


import logging

logger = logging.getLogger("normalize_logger")
logger.setLevel(logging.DEBUG)  # Show all logs (DEBUG and above)

if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def add_fixed_stub(record, shared_data):
    """
    Appends one or more fixed stub values to the end of each record.

    Expects:
        - shared_data["fixed_stub_fields"]: list of values (strings) to append
    """
    fixed_fields = shared_data.get("fixed_stub_fields", [])

    # Log if debugging is enabled
    logger = shared_data.get("logger")
    if logger:
        logger.debug(f"[5x_FixedStub] Original: {record}")
        logger.debug(f"[5x_FixedStub] Appending fixed fields: {fixed_fields}")

    return record + fixed_fields

def prepare_stub_key_config(stub_lookup, match_fields, logger=None, pad_widths=None, key_names=None):
    """
    Convert loader's composite keys into tuple-of-strings, apply symmetric
    normalization (zero-pad numeric parts to configured widths), and sort.
    """
    if logger is None:
        logger = logging.getLogger("normalize_logger")

    normalized_stub_lookup = {}

    for raw_key, stub_rows in stub_lookup.items():
        try:
            parts = [p.strip() for p in str(raw_key).split('||')]

            # Symmetric normalization for STUB keys
            if pad_widths and key_names:
                norm_parts = []
                for idx, s in enumerate(parts):
                    name = key_names[idx] if idx < len(key_names) else None
                    w = pad_widths.get(name, 0) if name else 0
                    if w and s.isdigit():
                        s = s.zfill(w)
                    norm_parts.append(s)
                parts = norm_parts

            norm_key = tuple(parts)
            normalized_stub_lookup[norm_key] = stub_rows

        except Exception as e:
            logger.warning(f"[STUB] Failed to process stub key {raw_key!r}: {e}")
            continue

    sorted_keys = sorted(normalized_stub_lookup.keys())
    logger.info("[Stub Logic] Stub keys normalized and sorted for high-low traversal.")
    return {
        "normalized_stub_lookup": normalized_stub_lookup,
        "sorted_stub_keys": sorted_keys
    }


def dispatch_stub_logic(record, config, record_index=None, log_fn=None):
    """
    Given an input record and a stub config, returns list of output records.
    Performs high-low matching logic: stub key held until data key >= stub key.
    """
    if not hasattr(log_fn, "debug"):
        log_fn = logging.getLogger("normalize_logger")

    key_fields = config.get("match_fields", [])
    stub_lookup = config.get("stub_lookup", {})
    # Ensure list semantics; dict default can break iteration below
    stub_fields = list(config.get("stub_fields", []))

    # Prepare references (produced by prepare_stub_key_config)
    stub_keys = config.get("sorted_stub_keys", [])
    normalized_stub_lookup = config.get("normalized_stub_lookup", {})

    # --- Build RAW composite key (no casting, preserve leading zeros) ---
    # --- Build composite key normalized to fixed pad widths (no casting) ---
    data_key = None
    try:
        log_fn.debug(f"[dispatch_stub_logic] Using key_fields indexes: {key_fields}")

        pad_widths = config.get("key_pad_widths", {}) or {}
        key_names_ordered = config.get("key_names", []) or []

        parts = []
        for pos, i in enumerate(key_fields):
            v = record[i] if i < len(record) else ""
            s = "" if v is None else str(v).strip()

            # Symmetric normalization: pad numeric strings to the agreed width
            name = key_names_ordered[pos] if pos < len(key_names_ordered) else None
            w = pad_widths.get(name, 0) if name else 0
            if w and s.isdigit():
                s = s.zfill(w)

            parts.append(s)

        data_key = tuple(parts)

        if record_index is not None and record_index < 5:
            log_fn.info(f"[DEBUG-STUB] Built data key: {data_key}")

    except Exception as e:
        log_fn.warning(f"[STUB] Failed to build data key: {e}")
        return [(record, False)]

    if record_index is not None and record_index < 5:
        log_fn.info(f"[STUB] Sample input record (raw) {record_index}: {record}")

    # ✅ High–low scan: ADVANCE ONLY; WRITE ONLY ON EQUALS (COBOL-style)
    if "stub_index" not in config:
        config["stub_index"] = 0  # Initialize once

    stub_index = config["stub_index"]
    match_found = False
    matching_key = None

    while stub_index < len(stub_keys):
        current_stub_key = stub_keys[stub_index]

        if current_stub_key < data_key:
            # stub is behind the data; advance stub
            stub_index += 1
            continue

        if current_stub_key > data_key:
            # data is behind the stub; no match for this row
            break

        # EQUALS: this is the ONLY time we consider it a match & write
        match_found = True
        matching_key = current_stub_key
        # Do NOT forcibly advance here; whether you advance depends on one-to-one vs one-to-many.
        # If you want one-to-one consumption, you can advance after consuming.
        break

    # persist the pointer
    config["stub_index"] = stub_index

    # Handle matched or unmatched result (EQUALS ONLY)
    if (not match_found) or (not matching_key) or (matching_key != data_key):
        if record_index is not None and record_index < 5:
            log_fn.info(f"[STUB-MATCH] No EQUAL match for key: {data_key}")
        return [(record, False)]

    # Fetch the (unique) stub row for this key (do not consume)
    stub_matches = normalized_stub_lookup.get(matching_key, [])
    if not stub_matches:
        return [(record, False)]

    # Optional sanity: warn if uniqueness is violated
    if len(stub_matches) > 1 and record_index is not None and record_index < 3:
        log_fn.warning(f"[STUB] Expected unique stub key; found {len(stub_matches)} rows for {matching_key}")

    stub_row = stub_matches[0]  # unique by contract

    # Build the appended values by name or index
    stub_values = []
    stub_names = config.get("stub_names", [])

    for sf_idx in stub_fields:
        try:
            if isinstance(stub_row, dict):
                # If sf_idx is an int, map it to a field name when we can
                key = stub_names[sf_idx] if isinstance(sf_idx, int) and stub_names else sf_idx
                stub_value = stub_row.get(key, "")
            else:
                # stub_row is a list/tuple; use numeric indexing
                stub_value = stub_row[sf_idx]
            stub_values.append(stub_value)
        except Exception as e:
            log_fn.warning(f"[dispatch_stub_logic] Failed to extract stub value at {sf_idx}: {e}")
            continue

    new_record = record + stub_values
    # Do NOT advance stub_index here; allow many data rows to match the same unique stub key.
    return [(new_record, True)]
