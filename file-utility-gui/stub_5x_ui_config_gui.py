"""
stub_5x_ui_config_gui.py
-----------------
Handles all user interaction required to define a new stub column.

Supports both fixed-value stubs and conditionally derived stubs based on:
- Date extraction (year/month)
- Substring presence
- Numeric range evaluation
- Match from list of values

Returns a structured configuration dictionary for use by 5x_Add_Stub.py.
"""
# 5x_Add_Stub_UI.py

import tkinter as tk
from tkinter import simpledialog, messagebox

def get_fixed_stub_column_config(default_value="APPENDED", column_name="Stub"):
    return {
        "mode": "fixed",
        "value": default_value,
        "new_column": column_name
    }


def get_fixed_stub_row(header_fields):
    """
    GUI prompt to collect one value per field from the user to form a fixed-value row.
    Returns a list representing the full row, or None if cancelled.
    """
    def on_submit():
        values = [entry.get().strip() for entry in entries]
        if any(v == "" for v in values):
            messagebox.showwarning("Incomplete", "All fields must be filled in.")
            return
        result.extend(values)
        dialog.destroy()

    result = []
    dialog = tk.Tk()
    dialog.title("Enter Fixed Row Values")

    tk.Label(dialog, text="Enter a value for each field:").grid(row=0, column=0, columnspan=2, pady=(10, 5))

    entries = []
    for i, field in enumerate(header_fields):
        tk.Label(dialog, text=f"{field}:", anchor="w").grid(row=i + 1, column=0, sticky="w", padx=10)
        entry = tk.Entry(dialog, width=40)
        entry.grid(row=i + 1, column=1, padx=10, pady=2)
        entries.append(entry)

    tk.Button(dialog, text="Submit", command=on_submit).grid(row=len(header_fields) + 1, column=0, columnspan=2, pady=10)

    dialog.mainloop()

    return result if result else None


def get_stub_column_config(header_fields, has_header):
    """
    Prompts the user to enter the name and value of a new column to append to all records.
    Returns a config dictionary with keys: 'new_column', 'value'
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main root window

    # Ask for new column name
    new_column = simpledialog.askstring(
        "Stub Column Name",
        "Enter name of new column to add to each record:"
    )
    if not new_column:
        messagebox.showwarning("Missing Input", "No column name provided.")
        return None

    # Ask for fixed value to insert
    value = simpledialog.askstring(
        "Stub Column Value",
        f"Enter fixed value for column '{new_column}':"
    )
    if value is None:
        messagebox.showwarning("Missing Input", "No value provided for stub column.")
        return None

    return {
        "new_column": new_column,
        "value": value
    }
