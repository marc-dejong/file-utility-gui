"""
stub_5x_ui_gui.py
--------------
Appends a new stub column to each record based on user-defined logic.

Supports both fixed and derived values, including:
- Fixed constant value for all records
- Extracted year or month from a date-formatted column
- Conditional value based on substring presence
- Range-based value from numeric comparisons
- Match against a list of acceptable values

Returns a modified row with the stub value appended.
Meant to be used within a streaming or chunk-based processing loop.
"""

from normalizer_core import normalize_streaming_record
import tkinter as tk
from tkinter import messagebox
from collections import deque
from stub_loader import load_stub_lookup_table_v2


def load_stub_lookup_table(config, logger=None):
    """
    Wrapper that uses the centralized loader (no casting),
    then adapts its output to the tuple-key -> [values] shape
    that the UI uses.
    """
    path = config["stub_file"]
    key_fields  = config.get("key_field_indices") or config.get("match_fields")
    stub_fields = config.get("stub_field_indices") or config.get("stub_fields")

    lookup, key_names, stub_names, enc, delim = load_stub_lookup_table_v2(
        stub_file_path=path,
        key_fields=key_fields,
        stub_fields=stub_fields,
        logger=logger,
        delimiter=';',     # set to None to sniff if needed
        encoding=None,
    )

    # Adapt to: dict[tuple(key_parts)] -> list[list_of_stub_values]
    # Adapt to: dict[tuple(key_parts)] -> deque[list_of_stub_values]
    stub_dict = {}
    for composite_key, rows in lookup.items():
        key_tuple = tuple(str(composite_key).split("||"))
        q = stub_dict.setdefault(key_tuple, deque())
        for row in rows:  # row is a dict keyed by stub field NAMES
            values = [row.get(name, "") for name in stub_names]
            q.append(values)

    if logger:
        sample = list(stub_dict.keys())[:5]
        logger.info(f"[UI] Stub table via v2: {len(stub_dict)} keys, enc={enc}, delim={repr(delim)}")
        logger.debug(f"[UI] Sample keys: {sample}")

    # (optional) store names if your UI needs them later
    config["key_names"]  = key_names
    config["stub_names"] = stub_names
    return stub_dict


def add_stub_from_lookup(row, key_value, stub_dict, log_fn=None):
    """
    Appends the next available stub values for the given key (FIFO).
    Expects stub_dict[key] to be a deque of value-lists.
    """
    try:
        matches = stub_dict.get(key_value)
        if isinstance(matches, deque) and matches:
            stub_vals = matches.popleft()
            return row + stub_vals, True

        if callable(log_fn):
            log_fn(f"[INFO] No stub match for key: {key_value}")
        return row, False

    except Exception as e:
        if callable(log_fn):
            log_fn(f"[ERROR] Stub lookup failed for key {key_value}: {e}")
        return row, False


def get_stub_column_config(header_fields):
    """
    Calls the GUI-based stub config prompt from stub_5x_ui_config_gui.
    """
    try:
        from stub_5x_ui_config_gui import get_stub_column_config as config_prompt
    except ImportError as e:
        messagebox.showerror("Import Error", f"Unable to import stub configuration:\n{e}")
        return None

    return config_prompt(header_fields)


def dispatch_stub_logic(row, config, stub_dict=None, record_index=None, normalize_config=None, log_fn=None):
    """
    Applies stub logic per record.
    Supports:
    - Fixed/conditional mode: calls add_stub_column()
    - From-file mode: matches on tuple keys, inserts ALL stub matches (multi-row expansion)

    Returns:
        list of (updated_row, matched_flag) tuples
    """
    try:
        import logging

        # Create or get the named stub logger
        logger = logging.getLogger("stub_logger")

        # Attach file handler from shared logger if available
        shared_logger = logging.getLogger("file_util")

        mode = config.get("mode", "fixed")
        logger.debug(f"[dispatch_stub_logic] Entered with mode={mode}, row[:5]={row[:5]}")

        if shared_logger.handlers and not getattr(logger, "_shared_handlers_added", False):
            for h in shared_logger.handlers:
                logger.addHandler(h)
            logger._shared_handlers_added = True  # guard against duplicates

        logger.setLevel(logging.DEBUG)
        logger.propagate = True  # Optional: lets messages bubble up to root logger too

        # Use fallback log_fn only if not passed
        if not callable(log_fn):
            log_fn = logger.info

        if mode == "from_file":
            key_indices = (
                    config.get("match_fields_main")
                    or config.get("key_field_indices")
                    or config.get("match_fields")
            )

            if not key_indices or stub_dict is None:
                logger.debug("[dispatch_stub_logic] Skipping row due to invalid config or missing stub_dict.")
                logger.debug(f"  key_indices: {key_indices}")
                logger.debug(f"  stub_dict is None: {stub_dict is None}")
                logger.debug(f"  config keys: {list(config.keys())}")
                return [(row, False)]

            # One-time readiness log (first row only)
            if record_index == 0 and stub_dict is not None:
                logger.info(f"[STUB] UI-path ready: {len(stub_dict)} keys loaded.")

            if any((not isinstance(i, int)) or i < 0 or i >= len(row) for i in key_indices):
                logger.debug(f"[WARN] Record {record_index} too short or invalid index for match key.")
                return [(row, False)]

            key_tuple = tuple((str(row[i]).strip() if i < len(row) else "") for i in key_indices)

            logger.debug(f"[dispatch_stub_logic] Extracted key_tuple from record: {key_tuple}")
            logger.debug(f"[dispatch_stub_logic] Attempting match for key_tuple: {key_tuple}")

            matches = stub_dict.get(key_tuple)

            if matches:
                logger.debug(f"[dispatch_stub_logic] Match FOUND for key_tuple: {key_tuple}")
            else:
                logger.debug(f"[dispatch_stub_logic] Match NOT found for key_tuple: {key_tuple}")

            if matches:
                results = []
                start_index = config.get("stub_col_start_index", 0)

                for match in matches:
                    inserted_values = match[start_index:] if start_index < len(match) else []
                    full_row = row + inserted_values
                    logger.debug(
                        f"[dispatch_stub_logic] Appending stub values: {inserted_values} to record index {record_index}")

                    if normalize_config:
                        try:
                            config_norm = {
                                "input_value_list": full_row,
                                "col_list": list(range(len(full_row))),
                                "field_names": [f"col_{i}" for i in range(len(full_row))],
                                "mode": "record"
                            }
                            full_row = normalize_streaming_record(config_norm, normalize_config, logger.debug)
                        except Exception as e:
                            logger.debug(f"[WARN] Normalization failed on stub expansion: {e}")

                    results.append((full_row, True))
                return results

            else:
                logger.debug(
                    f"[dispatch_stub_logic] No stub match found for key: {key_tuple} — passing record {record_index} unchanged")
                return [(row, False)]

        else:
            updated_row, success = add_stub_column(row, config, record_index, normalize_config, logger.debug)
            return [(updated_row, success)]

    except Exception as e:
        logger.debug(f"[ERROR] dispatch_stub_logic failure (row {record_index}): {e}")
        return [(row, False)]


def gui_multi_select_fields(title, header_fields):
    """
    Displays a listbox where users can select multiple fields in custom order.
    Captures the order in which the user clicks fields (not listbox order).
    """
    win = tk.Toplevel()
    win.title(title)
    win.geometry("400x450")

    tk.Label(win, text="Select one or more fields:").pack(pady=10)
    listbox = tk.Listbox(win, selectmode=tk.MULTIPLE, exportselection=False, height=20)
    for i, name in enumerate(header_fields):
        listbox.insert(tk.END, f"{i}: {name}")
    listbox.pack(padx=10, pady=5)

    selected_indices = []

    def on_select(event):
        widget = event.widget
        selections = widget.curselection()
        for s in selections:
            idx = int(widget.get(s).split(":")[0])
            if idx not in selected_indices:
                selected_indices.append(idx)

    listbox.bind("<<ListboxSelect>>", on_select)

    def confirm():
        if not selected_indices:
            messagebox.showerror("No Selection", "Please select at least one field.")
            return
        win.destroy()

    tk.Button(win, text="Confirm", command=confirm).pack(pady=10)
    win.grab_set()
    win.wait_window()
    return selected_indices if selected_indices else None


def get_stub_file_mapping_config_from_gui_mapping(mapping, logger=None):
    """
    Converts field name mapping returned from GUI into index-based stub_config.
    Expects mapping = {
        'match_fields': [...names from input file...],
        'stub_fields': [...names from stub file...],
        'input_header': [...],
        'stub_header': [...],
        'stub_file': ...,  # optional
        ...
    }
    """
    input_header = mapping.get("input_header", [])
    stub_header = mapping.get("stub_header", [])
    match_names = mapping.get("match_fields", [])
    stub_names = mapping.get("stub_fields", [])

    try:
        match_fields = [input_header.index(name) for name in match_names]
        stub_fields = [stub_header.index(name) for name in stub_names]
    except ValueError as e:
        if logger:
            logger.error(f"[STUB] Failed to map field names to indexes: {e}")
        raise

    config = {
        "match_fields": match_fields,
        "stub_fields": stub_fields,
        "mode": "from_file",
        "stub_file": mapping.get("stub_file"),
        "main_delimiter": mapping.get("main_delimiter", ";"),
        "main_has_header": mapping.get("main_has_header", True),
        "input_file": mapping.get("input_file"),
        "output_file": mapping.get("output_file"),
    }

    if logger:
        logger.debug(f"[STUB] Converted match_fields: {match_fields}")
        logger.debug(f"[STUB] Converted stub_fields: {stub_fields}")

    return config


def add_stub_column(row, config, record_index=None, normalize_config=None, log_fn=None):
    """
    Adds a stub column based on config logic.
    Supports:
        - Fixed values
        - Text condition (contains)
        - Numeric condition (>, <, etc.)
    """
    try:

        condition_type = config.get("condition_type", "fixed")
        stub_value = ""

        if condition_type == "fixed":
            stub_value = config.get("value", "")

        elif condition_type == "text":
            source_col = config.get("source_col")
            match_text = config.get("match_text", "")
            if 0 <= source_col < len(row) and match_text.lower() in row[source_col].lower():
                stub_value = config.get("value", "")
            else:
                stub_value = ""

        elif condition_type == "numeric":
            source_col = config.get("source_col")
            op_token = config.get("operator", "")
            threshold = config.get("threshold")

            try:
                from operator import lt, le, gt, ge, eq, ne
                OPS = {">": gt, ">=": ge, "<": lt, "<=": le, "==": eq, "!=": ne}

                if source_col is None or source_col >= len(row):
                    raise IndexError("source_col out of range")

                field_value = float(row[source_col])
                op = OPS.get(op_token)
                if op is None:
                    raise ValueError(f"Unsupported operator: {op_token}")

                stub_value = config.get("value", "") if op(field_value, float(threshold)) else ""
            except Exception as e:
                if callable(log_fn):
                    log_fn(f"[WARN] Failed to evaluate numeric condition at record {record_index}: {e}")
                return row, False


        else:
            if callable(log_fn):
                log_fn(f"[WARN] Unknown condition_type '{condition_type}' at record {record_index}")
            return row, False

        if isinstance(stub_value, list):
            updated_row = row + stub_value
        else:
            updated_row = row + [stub_value]

        if normalize_config:
            try:
                config_norm = {
                    "input_value_list": updated_row,
                    "col_list": list(range(len(updated_row))),
                    "field_names": [f"col_{i}" for i in range(len(updated_row))],
                    "mode": "record"
                }
                updated_row = normalize_streaming_record(config_norm, normalize_config, log_fn)
            except Exception as e:
                if callable(log_fn):
                    log_fn(f"[WARN] Normalization failed in add_stub_column: {e}")

        return updated_row, True



    except Exception as e:
        if callable(log_fn):
            log_fn(f"[ERROR] Failed to add stub at record {record_index}: {e}")
        return row, False




