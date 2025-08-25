from linkage_helpers import (
    guess_type_list,
    detect_sign_styles,
    analyze_lengths_from_df,
    init_length_tracker,
    update_length_tracker,
    assign_default_flags
)

def build_linkage_dict():
    """
    Returns a blank linkage dictionary used for field normalization.
    """
    return {
        "col_list": [],
        "type_list": [],
        "length_list": [],
        "precision_list": [],
        "input_value_list": [],
        "output_value_list": [],
        "normalize_flags": [],
        "field_names": [],
        "mode": None,
        "source_file_path": None,
        "sign_style_list": [],
    }


def validate_linkage_dict(linkage):
    """
    Validates that all required fields in the linkage dictionary are present
    and consistent in length where required.

    Returns:
        (bool, str): Tuple of (is_valid, message). If valid, message is "OK".
    """
    required_keys = [
        "col_list", "type_list", "length_list", "input_value_list", "output_value_list"
    ]

    # Check required keys exist
    for key in required_keys:
        if key not in linkage:
            return False, f"[ERROR] Missing required field in linkage: '{key}'"

    # Check list lengths match for aligned fields
    list_fields = ["col_list", "type_list", "length_list", "input_value_list"]
    lengths = [len(linkage[k]) for k in list_fields]
    if len(set(lengths)) > 1:
        return False, f"[ERROR] Mismatched list lengths: {dict(zip(list_fields, lengths))}"

    # Optional: Validate types
    for i, dtype in enumerate(linkage["type_list"]):
        if dtype not in ("int", "float", "str"):
            return False, f"[ERROR] Invalid type '{dtype}' at position {i} in type_list"

    return True, "OK"


def generate_linkage_config(source, field_names, mode="record", dtype_hints=None):

    """
    Builds a normalization linkage dictionary based on sample data.

    Args:
        source (list or DataFrame): Sample records or Dask DataFrame
        field_names (list): Names of the fields
        mode (str): 'record', 'stream', or 'batch'

    Returns:
        dict: Normalization linkage dictionary
    """
    num_fields = len(field_names)
    col_list = list(range(num_fields))

    if mode == "batch":
        # --- Expecting a Dask DataFrame ---
        lengths, precisions = analyze_lengths_from_df(source)
        type_list = ["str"] * num_fields  # Optional: derive from schema
        sign_styles = ["leading"] * num_fields

    else:
        # --- Expecting sample records: list[list[str]] ---
        # Defensive: ensure source is list of lists and non-empty
        if isinstance(source, list) and source and isinstance(source[0], list):
            sample_row = source[0]
        else:
            sample_row = []

        type_list = guess_type_list(sample_row)
        sign_styles = detect_sign_styles(sample_row)

        tracker = init_length_tracker(num_fields)
        update_length_tracker(tracker, sample_row)

        lengths = tracker["length_list"]
        precisions = tracker["precision_list"]

    # --- Apply optional dtype_hints (type + flags) ---

    # 1) Override inferred types if a hint provides one
    if dtype_hints:
        for idx, hint in dtype_hints.items():
            if 0 <= idx < num_fields:
                htype = (hint or {}).get("type")
                if htype in ("int", "float", "str"):
                    type_list[idx] = htype

    # 2) Compute default flags from the (possibly updated) type_list
    flags = assign_default_flags(type_list)

    # 3) Overlay any explicit flag hints (string or first of list)
    if dtype_hints:
        for idx, hint in dtype_hints.items():
            if 0 <= idx < num_fields:
                hflag = (hint or {}).get("flags")
                if isinstance(hflag, list):
                    hflag = hflag[0] if hflag else None
                if isinstance(hflag, str) and hflag:
                    flags[idx] = hflag

    linkage = {
        "col_list": col_list,
        "type_list": type_list,
        "length_list": lengths,
        "precision_list": precisions,
        "field_names": field_names,
        "normalize_flags": flags,  # <-- use merged flags
        "sign_style_list": sign_styles,
        "mode": mode
    }

    return linkage

