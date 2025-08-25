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
import chardet
from tkinter import filedialog, messagebox, simpledialog
import os
import traceback
import logging


def safe_open(filepath, mode="r", newline=None, flexible=True, **kwargs):
    kwargs.pop("flexible", None)  # ← removes flexible before passing to open()
    """
    Attempts to open a file with a series of fallback encodings.
    Returns: (file object, encoding used, was_replaced)
    """
    # Remove any accidental 'flexible' in kwargs passed through **kwargs
    kwargs.pop("flexible", None)

    encodings_to_try = ["utf-8", "cp1252", "latin1"]
    was_replaced = False
    print(f"[DEBUG] flexible={flexible}")

    for enc in encodings_to_try:
        try:
            f = open(filepath, mode, encoding=enc, newline=newline, **kwargs)
            if 'r' in mode:
                try:
                    for _ in f:
                        pass
                    f.seek(0)
                except UnicodeDecodeError:
                    f.close()
                    continue
            return f, enc, was_replaced

        except Exception:
            continue

    # Final fallback using utf-8 with replacement only if flexible is enabled
    if flexible:
        try:
            f = open(filepath, mode, encoding="utf-8", errors="replace", newline=newline, **kwargs)
            was_replaced = True
            print(f"[WARN] Used utf-8 with replacement on: {filepath}")
            return f, "utf-8 (errors=replace)", was_replaced
        except Exception:
            pass

    raise UnicodeDecodeError(f"Could not decode {filepath} with supported encodings.")



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
        column_list = "\n".join([f"{idx}: {col}" for idx, col in enumerate(valid_columns)])
        messagebox.showinfo("Available Columns", column_list)

        column_input = simpledialog.askstring(
            "Select Date Column",
            "Enter the column number or name to evaluate as a date:"
        )
        if not column_input:
            messagebox.showerror("Input Error", "A column selection is required.")
            return None

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

    choice = simpledialog.askstring(
        "Date Filter Type",
        "Select the type of date filter to apply:\n"
        "1. Exact date match\n"
        "2. Date is on or after a specified date\n"
        "3. Date is on or before a specified date\n"
        "4. Date falls within a range (inclusive)\n"
        "5. Cancel"
    )

    filter_type = filter_type_options.get(choice)
    if not filter_type:
        messagebox.showerror("Invalid Selection", "Please enter a number between 1 and 5.")
        return None

    if filter_type == "cancel":
        messagebox.showinfo("Cancelled", "Date filter configuration aborted.")
        return None

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

    format_result = format_options.get(choice)
    if not format_result:
        messagebox.showerror("Invalid Selection", "Please enter a valid option number (1–9).")
        return None

    if choice == "9":  # cancel
        messagebox.showinfo("Cancelled", "Date filter configuration aborted.")
        return None

    date_format, format_label = format_result

    format_text = "\n".join([f"{key}. {label}" for key, (_, label) in format_options.items()])
    format_choice = simpledialog.askstring(
        "Date Format",
        f"Select date format:\n\n{format_text}"
    )

    fmt_code, fmt_label = format_options.get(format_choice, (None, None))
    if not fmt_code:
        messagebox.showerror("Invalid Selection", "Please enter a valid option (1–9).")
        return None

    if fmt_code == "cancel":
        messagebox.showinfo("Cancelled", "Date filter configuration aborted.")
        return None

    source_format = fmt_label

    # Optional custom format string
    if fmt_code == "custom":
        fmt_code = simpledialog.askstring(
            "Custom Format",
            "Enter the Python datetime format string (e.g., %m-%d-%Y):"
        )
        if not fmt_code:
            messagebox.showerror("Input Error", "Custom format is required.")
            return None
        source_format = fmt_code

    # Step 3: Date input (raw values only for now)
    raw_input_start = None
    raw_input_end = None

    if filter_type in ("exact", "after", "before"):
        raw_input_start = simpledialog.askstring(
            "Date Entry",
            f"Enter the date (format: {fmt_label}):"
        )
        if not raw_input_start:
            messagebox.showerror("Input Error", "Date value is required.")
            return None

    elif filter_type == "range":
        raw_input_start = simpledialog.askstring(
            "Start Date",
            f"Enter the start date (format: {fmt_label}):"
        )
        raw_input_end = simpledialog.askstring(
            "End Date",
            f"Enter the end date (format: {fmt_label}):"
        )
        if not raw_input_start or not raw_input_end:
            messagebox.showerror("Input Error", "Both start and end dates are required.")
            return None

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
                traceback_str = traceback.format_exc()
                logging.error("Date parsing failed:\n%s", traceback_str)
                messagebox.showerror("Date Parsing Error", f"Failed to parse date(s):\n{e}")

                retry = messagebox.askyesno("Retry?", "Would you like to re-enter the date filter values?")
                if retry:
                    return prompt_for_date_filter_config()
                else:
                    messagebox.showinfo("Cancelled", "Date filter configuration cancelled.")
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
    path = filedialog.askopenfilename(title=prompt_msg)
    if not path or not os.path.isfile(path):
        messagebox.showerror("File Not Found", f"The selected file was not found:\n{path}")
        return prompt_for_file_path(prompt_msg)  # Retry until valid
    return path


def prompt_for_delimiter() -> str:
    delim = simpledialog.askstring(
        "Delimiter",
        "Enter the delimiter character used in your file:\n(e.g., ',' for CSV, '\\t' for tab)"
    )
    if not delim:
        messagebox.showerror("Invalid Input", "A delimiter is required.")
        return prompt_for_delimiter()
    return delim


def prompt_yes_no(prompt_msg: str) -> bool:
    return messagebox.askyesno("Confirm", prompt_msg)

def display_column_choices(header_list: list):
    column_text = "\n".join([f"{i}: {col}" for i, col in enumerate(header_list)])
    messagebox.showinfo("Available Columns", column_text)



