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

import re

# 5x_Add_Stub.py

"""
Module: 5x_Add_Stub.py
Description:
Provides functionality to add a new stub column to each record based on a configuration
dictionary. Supports fixed-value stubs and conditionally-derived stubs using:
- Date extraction (year or month)
- Substring presence tests
- Numeric range evaluations
- Matching from a list of text values
Functions:
- add_stub_to_row: Appends the stub value to a given record row.
"""

def add_stub_column(row, header_fields, config, log_fn=None):
    """
    Appends a fixed-value stub column to the provided row.
    Returns (updated_row, was_matched=True)
    """
    stub_value = config.get("value", "")
    return row + [stub_value], True



