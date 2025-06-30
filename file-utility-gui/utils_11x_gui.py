"""
MODULE: utils_11x.py.py

DESCRIPTION:
    This module contains general-purpose utility functions used throughout
    the Multi-Function File Utility Suite. These shared routines include
    user input helpers, data validation tools, and reusable components
    that support modules such as filtering, replacement, stub addition,
    and date-based record selection.

    All functions in this module are designed for reuse across multiple
    processing layers, with standardized error handling and logging.
"""
from datetime import datetime
import sys
import os
import chardet

import os
import chardet

def safe_open(filepath, mode="r", newline=None, flexible=True):

    """
    Attempts to open a file with a series of fallback encodings.
    Returns: (file object, encoding used, was_replaced)
    """
    encodings_to_try = ["utf-8", "cp1252", "latin1"]
    was_replaced = False
    print( f"[DEBUG] flexible={flexible}" )

    for enc in encodings_to_try:
        try:
            f = open( filepath, mode, encoding=enc, newline=newline )
            if 'r' in mode:
                try:
                    # Force full decode to catch mid-file encoding issues
                    for _ in f:
                        pass
                    f.seek( 0 )
                except UnicodeDecodeError:
                    f.close()
                    continue  # Try next encoding
            return f, enc, was_replaced
        except Exception:
            continue  # Try next encoding

    # Final fallback using utf-8 with replacement only if flexible is enabled
    if flexible:
        try:
            f = open( filepath, mode, encoding="utf-8", errors="replace", newline=newline )
            was_replaced = True
            print( f"[WARN] Used utf-8 with replacement on: {filepath}" )
            return f, "utf-8 (errors=replace)", was_replaced
        except Exception:
            pass  # Let the final failure block catch it


def prompt_for_date_filter_config(valid_columns: list[str]) -> dict:
    """
    Prompts the user to configure a date-based filter.

    Args:
        valid_columns (list[str]): A list of available column names from the input file.
                                   The user will be asked to select or type a column name from this list.

    Returns:
        dict: A dictionary containing the validated date filter configuration.
    """

    print("\n[DATE FILTER CONFIGURATION]")

    # Step 1: Show fields and ask for a column by number or name
    while True:
        print("\nAvailable columns for date filtering:")
        for idx, col in enumerate(valid_columns):
            print(f"  {idx}: {col}")

        column_input = input("Enter the column number or name to evaluate as a date: ").strip()

        if column_input.isdigit():
            index = int(column_input)
            if 0 <= index < len(valid_columns):
                column_name = valid_columns[index]
                break
            else:
                print(f"[ERROR] Column index '{index}' is out of range.")
        elif column_input in valid_columns:
            column_name = column_input
            break
        else:
            print(f"[ERROR] '{column_input}' not found in column names. Please try again.")

    filter_type_options = {
        "1": "exact",
        "2": "after",
        "3": "before",
        "4": "range",
        "5": "cancel"
    }

    while True:
        print("\nSelect the type of date filter to apply:")
        print("  1. Exact date match")
        print("  2. Date is on or after a specified date")
        print("  3. Date is on or before a specified date")
        print("  4. Date falls within a range (inclusive)")
        print("  5. Cancel")
        choice = input("> ").strip()
        filter_type = filter_type_options.get(choice)
        if filter_type:
            if filter_type == "cancel":
                print("[CANCELLED] Date filter configuration aborted by user.")
                sys.exit(0)
            break
        print("[ERROR] Invalid selection. Please enter 1-5.\n")

    # Step 4: Select source format
    format_options = {
        "1": ("%Y-%m-%d", "YYYY-MM-DD"),
        "2": ("%Y-%m-%d %H:%M:%S.%f", "YYYY-MM-DD HH:MM:SS.SSS"),
        "3": ("%m-%d-%Y", "MM-DD-YYYY"),
        "4": ("%m/%d/%Y", "MM/DD/YYYY"),
        "5": ("excel", "Excel Serial (e.g., 45555)"),
        "6": ("julian", "Julian (YYDDD, e.g., 24139)"),
        "7": ("prefixed_julian", "Prefixed Julian (e.g., DT25139)"),
        "8": ("custom", "Custom Format (manual entry)"),
        "9": ("cancel", "Cancel")
    }

    print( "\nWhat format are the dates in this column?" )
    for key, (_, label) in format_options.items():
        print( f"  {key}. {label}" )

    while True:
        format_choice = input( "> " ).strip()
        fmt_code, fmt_label = format_options.get( format_choice, (None, None) )
        if fmt_code:
            if fmt_code == "cancel":
                print( "[CANCELLED] Date filter configuration aborted by user." )
                sys.exit( 0 )
            break
        print( "[ERROR] Invalid selection. Please enter 1-9.\n" )

    source_format = fmt_label

    # Optional custom format string
    if fmt_code == "custom":
        fmt_code = input( "\nEnter the Python datetime format string (e.g., %m-%d-%Y): " ).strip()
        source_format = fmt_code

    # Step 3: Date input (raw values only for now)
    raw_input_start = None
    raw_input_end = None

    if filter_type in ("exact", "after", "before"):
        raw_input_start = input( f"\nEnter the date (format: {fmt_label}): " ).strip()

    elif filter_type == "range":
        raw_input_start = input( f"\nEnter the start date (format: {fmt_label}): " ).strip()  # <-- CHANGED
        raw_input_end = input( f"Enter the end date (format: {fmt_label}): " ).strip()

    return {
        "column_name": column_name,
        "filter_type": filter_type,
        "start_date": raw_input_start,  # Will be parsed later
        "end_date": raw_input_end,
        "source_format": source_format,
        "format_code": fmt_code,        # Internal use for parsing
        "raw_input_start": raw_input_start,
        "raw_input_end": raw_input_end,
        "exclusive": False
    }

def validate_and_parse_date_config(config: dict) -> dict:
    """
    Validates and parses the raw date strings in the date filter configuration.

    Args:
        config (dict): A dictionary returned by prompt_for_date_filter_config(),
                       containing raw input strings and format metadata.

    Returns:
        dict: The same dictionary with added keys:
            - start_date (datetime): Parsed from raw_input_start
            - end_date (datetime or None): Parsed from raw_input_end (if applicable)

    Behavior:
        - Parses using the selected format code (e.g., %Y-%m-%d or Excel/Julian logic)
        - Prompts user to retry or exit if parsing fails
    """
    from datetime import datetime, timedelta

    def parse_single_date(raw_value: str, fmt_code: str) -> datetime:
        while True:
            try:
                if fmt_code == "excel":
                    base_date = datetime(1899, 12, 30)
                    return base_date + timedelta(days=int(raw_value))
                elif fmt_code == "julian":
                    year = int(raw_value[:2]) + 2000
                    day_of_year = int(raw_value[2:])
                    return datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
                elif fmt_code == "prefixed_julian":
                    stripped = ''.join(filter(str.isdigit, raw_value))
                    year = int(stripped[:2]) + 2000
                    day_of_year = int(stripped[2:])
                    return datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
                else:
                    return datetime.strptime(raw_value, fmt_code)

            except Exception as e:
                print(f"\n[ERROR] Failed to parse date '{raw_value}' using format '{fmt_code}'.")
                print("Error details:", str(e))
                retry = input("Would you like to re-enter the date? (y/n): ").strip().lower()
                if retry == "y":
                    raw_value = input(f"Re-enter the date (format: {fmt_code}): ").strip()
                else:
                    print("\n[FAILURE] Date parsing aborted by user.")
                    return None

    # Parse start date
    config["start_date"] = parse_single_date(config["raw_input_start"], config["format_code"])
    if config["start_date"] is None:
        sys.exit(1)

    # Parse end date if range
    if config["filter_type"] == "range":
        config["end_date"] = parse_single_date(config["raw_input_end"], config["format_code"])
        if config["end_date"] is None:
            sys.exit(1)

    return config

def prompt_for_file_path(prompt_msg: str) -> str:
    while True:
        path = input(f"{prompt_msg}\n> ").strip()
        if os.path.isfile(path):
            return path
        print(f"[ERROR] File not found: {path}")

def prompt_for_delimiter(prompt_msg: str = "Enter the delimiter used in the file (e.g., , ; |):") -> str:
    while True:
        delimiter = input(f"{prompt_msg}\n> ").strip()
        if delimiter:
            return delimiter
        print("[ERROR] Delimiter cannot be empty.")

def prompt_yes_no(prompt_msg: str) -> bool:
    while True:
        response = input(f"{prompt_msg} (y/n): ").strip().lower()
        if response in {"y", "yes"}:
            return True
        elif response in {"n", "no"}:
            return False
        else:
            print("[ERROR] Please enter 'y' or 'n'.")

def display_column_choices(headers: list[str]):
    print("\nAvailable Columns:")
    for idx, name in enumerate(headers):
        print(f"[{idx}] {name}")



