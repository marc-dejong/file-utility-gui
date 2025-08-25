"""
normalizer_core.py

This module provides the core logic for normalizing field values
based on type, width, precision, and optional sign formatting.

It expects a structured linkage dictionary (from normalizer_linkage.py)
that defines the configuration and raw values to normalize.

Main Function:
- normalize_record(linkage_dict): Applies normalization to input values and returns updated output.

Author: Marc DeJong
Created: 2025-07-15
Version: 1.0

Change Log:
----------------------------------------------------------------------
| Date       | Description                                           |
|------------|-------------------------------------------------------|
| 2025-07-16 | Integrated linkage_helpers: type/sign/length analysis |
|            | Added normalize_flags logic (strict/pad_only/skip)   |
----------------------------------------------------------------------
"""

import logging
logger = logging.getLogger("normalizer_core")
import traceback
import os

# --- Generic, name-based dtype hints (works for any file) ---
def derive_dtype_hints(field_names):
    """
    Return a dict {index: {"type": "str"/"int"/"float", "flags": [...]}}
    based on lightweight name heuristics. Safe defaults; never raises.
    """
    hints = {}
    for i, raw in enumerate(field_names or []):
        name = str(raw or "").strip().lower()

        # Postal codes: keep strings (handles US/CA/UK formats and leading zeros)
        if "zip" in name or "postal" in name or "postcode" in name:
            hints[i] = {"type": "str", "flags": ["pad_only"]}

        # 'year' : integer but allow blanks → no errors on ''
        elif name.endswith("year") or " year" in name or name == "year":
            hints[i] = {"type": "int", "flags": ["nullable_int"]}

        # Money-ish columns: float
        elif any(tok in name for tok in ("amount", "amt", "total", "balance", "bpi")):
            hints[i] = {"type": "float", "flags": []}

        # State/Province abbreviations: keep strings
        elif any(tok in name for tok in ("state", "province", "abbr", "code")):
            # Don't force 'int' just because 'code' appears
            hints[i] = {"type": "str", "flags": []}

        # default: no hint → normal inference stands
    return hints

# --- Setup dedicated logger for normalization warnings ---
normalize_logger = logging.getLogger("normalize_logger")
normalize_logger.setLevel(logging.WARNING)

if not normalize_logger.hasHandlers():
    file_handler = logging.FileHandler("normalization_warnings.log", mode='w')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    normalize_logger.addHandler(file_handler)

from normalizer_linkage import generate_linkage_config
from linkage_helpers import (
    guess_type_list,
    detect_sign_styles,
    analyze_lengths_from_df,
    init_length_tracker,
    update_length_tracker,
    assign_default_flags
)

def build_normalizer_config(shared_data):
    """
    Consolidates normalization configuration for both streaming and Dask modes.

    Args:
        shared_data (dict): The shared application state dictionary.

    Returns:
        dict: A configuration dictionary to be passed to the normalization routines.
    """
    config = {}

    # Required field list
    config["col_list"] = shared_data.get("normalizer_col_list", [])
    config["field_names"] = config["col_list"]  # optional but useful

    # File info
    input_file = shared_data.get("input_file")
    if isinstance(input_file, str):
        config["source_file_path"] = input_file
    else:
        config["source_file_path"] = "unknown_input_file"

    config["delimiter"] = shared_data.get("delimiter", ",")
    config["header"] = shared_data.get("has_header", False)

    # Flags
    flags = shared_data.get("flags", {})
    config["strip_quotes"] = flags.get("strip_quotes", False)
    config["debug"] = flags.get("debug", False)

    # Optional future add-ons
    # config["normalize_flags"] = shared_data.get("normalize_flags", [])
    # config["sign_style_list"] = shared_data.get("sign_style_list", [])

    return config

def normalize_streaming_record(record, config):
    """
    Normalizes a single record in streaming mode using pre-sampled config.

    Args:
        record (list): A single record (list of string values)
        config (dict): Must contain col_list, sample_records, etc.

    Returns:
        list: Normalized record
    """
    try:
        col_list = config.get("col_list", [])
        sample_records = config.get("sample_records", [])

        # Fallback field names if not provided
        field_names = config.get("field_names")
        if not field_names:
            # prefer col_list size; fall back to the record length
            n = len(col_list) if col_list else len(record)
            field_names = [f"col_{i}" for i in range(n)]

        if not sample_records:
            raise ValueError("No sample records provided for streaming normalization.")

        # Name-based hints (generic / schema-agnostic)
        dtype_hints = derive_dtype_hints(field_names)

        # Build linkage from samples (stream mode)
        linkage = generate_linkage_config(
            sample_records,
            field_names,
            mode="stream",
            dtype_hints=dtype_hints
        )

        # Ensure input_value_list matches the selected columns/order
        if not col_list:
            col_list = list(range(len(record)))  # safe default: all columns in order
        linkage["col_list"] = col_list
        linkage["field_names"] = field_names
        linkage["input_value_list"] = [
            str(record[i]) if i < len(record) else ""   # guard short rows
            for i in col_list
        ]

        normalized = normalize_record(linkage, shared_data=config.get("shared_data"))
        return normalized["output_value_list"]

    except Exception as e:
        # Use the module logger (consistent with the rest of the file)
        logger.error(f"[normalize_streaming_record] Failed to normalize record: {e}")
        # Print traceback if available; avoid secondary errors if traceback isn't imported
        try:
            import traceback
            traceback.print_exc()
        except Exception:
            pass
        return record  # fail-safe passthrough


def normalize_dask_df(df, config):
    """
    Normalizes an entire Dask DataFrame using the batch normalization mode.
    """
    normalize_logger.info("Starting batch normalization on Dask DataFrame")

    field_names = config.get("col_list", [])
    dtype_hints = derive_dtype_hints(field_names)
    linkage_template = generate_linkage_config(
        df,
        field_names,
        mode="batch",
        dtype_hints=dtype_hints
    )

    normalized = df.map_partitions(
        lambda partition: partition.apply(
            lambda row: normalize_record({
                **linkage_template,
                "input_value_list": [str(row.iloc[col]) for col in linkage_template["col_list"]],
                "source_df": df
            }, shared_data=config.get("shared_data"))["output_value_list"],
            axis=1,
            result_type="expand"
        )
    )

    return normalized  # ✅


def normalize_record(linkage_dict, shared_data=None):
    """
    Applies normalization to the input_value_list based on the type_list,
    length_list, and precision_list (if applicable).

    Updates output_value_list in-place.

    Args:
        linkage_dict (dict): Normalization linkage dictionary.

    Returns:
        dict: Updated linkage dictionary with output_value_list filled in.
    """
    logger = shared_data.get("logger") if shared_data else normalize_logger

    input_vals = linkage_dict["input_value_list"]
    output_vals = []

    # --- Extract input values ---
    input_vals = linkage_dict.get("input_value_list")
    if not isinstance(input_vals, list) or not input_vals:
        raise ValueError("[ERROR] Missing or invalid 'input_value_list'.")

    # --- Detect or assign type_list if missing ---
    if not linkage_dict.get("type_list"):
        linkage_dict["type_list"] = guess_type_list(input_vals)

    # --- Detect sign styles ---
    if not linkage_dict.get("sign_style_list"):
        linkage_dict["sign_style_list"] = detect_sign_styles(input_vals)

    # --- Handle length/precision based on mode ---
    mode = linkage_dict.get("mode", "record")
    normalize_flags = linkage_dict.get("normalize_flags", [])
    type_list = linkage_dict.get("type_list", [])
    length_list = linkage_dict.get("length_list", [])
    precision_list = linkage_dict.get("precision_list", [])
    sign_style_list = linkage_dict.get("sign_style_list", [])

    if mode == "record":
        tracker = init_length_tracker(len(linkage_dict["type_list"]))
        update_length_tracker(tracker, input_vals)  # don't reassign
        linkage_dict["length_list"] = tracker["length_list"]
        linkage_dict["precision_list"] = tracker["precision_list"]

    elif mode == "batch":
        df = linkage_dict.get("source_df")
        if df is None:
            raise ValueError("[ERROR] source_df is required for batch normalization.")

        lengths, precisions = analyze_lengths_from_df(df)
        linkage_dict["length_list"] = lengths
        linkage_dict["precision_list"] = precisions

    elif mode == "stream":
        # Stream mode uses same logic as record-level
        norm_output = []
        for i, raw_val in enumerate(input_vals):
            flag = normalize_flags[i]
            dtype = type_list[i]
            length = length_list[i]
            precision = precision_list[i]
            sign_style = sign_style_list[i]

            if flag == "skip":
                norm_output.append(str(raw_val))
                continue
            elif flag == "pad_only":
                norm = str(raw_val).strip().ljust(length)
                norm_output.append(norm)
                continue

            if flag == "nullable_int" and dtype == "int":
                txt = str(raw_val).strip()
                if txt == "":
                    norm_output.append("")  # keep blank
                    continue

            try:
                if dtype == "int":
                    norm = str(int(str(raw_val).strip())).rjust(length)
                elif dtype == "float":
                    raw_num = float(str(raw_val).replace(",", "").replace("$", ""))
                    if sign_style == "paren" and raw_val.strip().startswith("("):
                        raw_num = -abs(raw_num)
                    formatted = f"{raw_num:,.{precision}f}"
                    norm = formatted.rjust(length)
                else:
                    norm = str(raw_val).strip().ljust(length)
                norm_output.append(norm)
            except Exception as e:
                str(raw_val)
                logger.warning(
                    f"[normalize_record] Stream-mode normalization failed at index {i}: "
                    f"val='{str(raw_val)}' dtype='{dtype}' length={length} precision={precision} sign_style='{sign_style}' — {e}"
                )
                norm_output.append(str(raw_val))

        return {
            "input_value_list": input_vals,
            "output_value_list": norm_output
        }

    else:
        raise ValueError(f"[ERROR] Unsupported mode: '{mode}'")

    # --- Assign normalization flags (default version) ---
    if not linkage_dict.get("normalize_flags"):
        linkage_dict["normalize_flags"] = assign_default_flags(linkage_dict["type_list"])

    # AFTER all helper logic is complete
    types = linkage_dict["type_list"]
    lengths = linkage_dict["length_list"]
    precisions = linkage_dict.get("precision_list", [])
    sign_styles = linkage_dict.get("sign_style_list", [])

    warning_counts = {}
    for i, raw_val in enumerate(input_vals):
        dtype = types[i]
        length = lengths[i]
        precision = precisions[i] if i < len(precisions) else None
        sign_style = sign_styles[i] if i < len(sign_styles) else "leading"

        flags = linkage_dict.get("normalize_flags", ["strict"] * len(input_vals))
        flag = flags[i]

        # --- Route based on flag ---
        if flag == "skip":
            output_vals.append(str(raw_val))
            continue
        elif flag == "pad_only":
            norm = str(raw_val).strip().ljust(length)
            output_vals.append(norm)
            continue

        # allow blanks for nullable int fields
        if flag == "nullable_int" and dtype == "int":
            if str(raw_val).strip() == "":
                output_vals.append("")  # keep blank, no warning
                continue

        # --- Log normalization mismatches or suspicious values ---
        field_name = linkage_dict.get("field_names", [f"col_{j}" for j in range(len(input_vals))])[i]

        if dtype in ("int", "float"):
            try:
                float(raw_val)
            except Exception:
                warning_counts[field_name] = warning_counts.get(field_name, 0) + 1
                logger.warning(f"[WARN] Field '{field_name}' expected {dtype}, got non-numeric: '{raw_val}'")

        if dtype == "int" and not str(raw_val).lstrip("0").isdigit():
            warning_counts[field_name] = warning_counts.get(field_name, 0) + 1
            logger.warning(f"[WARN] Field '{field_name}' expected integer, got: '{raw_val}'")

        if len(str(raw_val)) > length:
            warning_counts[field_name] = warning_counts.get(field_name, 0) + 1
            logger.warning(
                f"[WARN] Field '{field_name}' input length {len(str(raw_val))} exceeds max {length}: '{raw_val}'")

        # --- Now do the normalization ---
        try:
            if dtype == "int":
                val = int(str(raw_val).strip())
                norm = str(val).rjust(length, "0")

            elif dtype == "float":
                val = float(str(raw_val).strip()
                            .replace("(", "-")
                            .replace(")", "")
                            .replace("$", "")
                            .replace(",", "")
                            .replace(" ", "")
                            .replace("-", "-"))

                # Normalize to standard format with fixed precision
                if precision is not None:
                    norm = f"{val:.{precision}f}".rjust(length, "0")
                else:
                    norm = str(val).rjust(length, "0")

            elif dtype == "str":
                norm = str(raw_val).strip()

            else:
                norm = str(raw_val)

        except Exception as e:
            norm = f"[ERROR: {e}]"

        output_vals.append(norm)

    if warning_counts:
        logger.warning("----- Normalization Warning Summary (per field) -----")
        for field, count in warning_counts.items():
            logger.warning(f"Field '{field}': {count} warning(s)")

    linkage_dict["output_value_list"] = output_vals
    return linkage_dict
