# match_mapper_gui.py
# Supports auto-matching of input file headers to stub file headers

import tkinter as tk
from tkinter import simpledialog, messagebox
from gui_field_selector import gui_select_fields  # Assumes this helper already exists

def get_field_mapping(input_header, stub_header, logger=None):
    """
    Launches two GUI dialogs:
    1. Select fields from INPUT to use for matching
    2. Select fields from STUB to insert into records
    Returns a dictionary with selected match/stub fields and headers.
    """
    if logger:
        logger.debug("[MAPPING] get_field_mapping started")

    if not input_header or not stub_header:
        if logger:
            logger.warning("[MAPPING] Missing headers — cannot perform field mapping.")
        return {}

    try:
        if logger:
            logger.debug("[MAPPING] Launching match field selector GUI")
        match_fields = gui_select_fields(
            title="Match Fields",
            prompt="Select fields from the INPUT file to use as matching keys:",
            header=input_header
        )

        if logger:
            logger.debug("[MAPPING] Launching stub field selector GUI")
        stub_fields = gui_select_fields(
            title="Stub Insert Fields",
            prompt="Select fields from the STUB file to insert into matching records:",
            header=stub_header
        )

        if not match_fields or not stub_fields:
            if logger:
                logger.warning("[MAPPING] No match or stub fields selected.")
            return {}

        result = {
            "match_fields": match_fields,
            "stub_fields": stub_fields,
            "input_header": input_header,
            "stub_header": stub_header
        }

        if logger:
            logger.debug(f"[MAPPING] Final field mapping result: {result}")

        return result

    except Exception as e:
        if logger:
            logger.error(f"[MAPPING] Failed during GUI field selection: {e}")
        messagebox.showerror("Error", f"Field mapping failed:\n{e}")
        return {}
