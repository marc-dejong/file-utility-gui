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
from normalizer_core import normalize_streaming_record


def get_fixed_stub_column_config(default_value="APPENDED", column_name="Stub"):
    return {
        "mode": "fixed",
        "value": default_value,
        "new_column": column_name
    }

def gui_select_field(title, header_fields):
    """
    Displays a simple listbox to select a single field. Returns the index selected.
    """
    win = tk.Toplevel()
    win.title(title)
    win.geometry("400x400")

    tk.Label(win, text="Select one field:").pack(pady=10)
    listbox = tk.Listbox(win, height=20, exportselection=False)
    for i, name in enumerate(header_fields):
        listbox.insert(tk.END, f"{i}: {name}")
    listbox.pack(padx=10, pady=5)

    selected_idx = []

    def confirm():
        sel = listbox.curselection()
        if not sel:
            messagebox.showerror("No Selection", "Please select a field.")
            return
        selected_idx.append(int(listbox.get(sel[0]).split(":")[0]))
        win.destroy()

    tk.Button(win, text="Confirm", command=confirm).pack(pady=10)

    win.grab_set()
    win.wait_window()
    return selected_idx[0] if selected_idx else None


def get_fixed_stub_row(header_fields, normalize_config=None, log_fn=None):

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

    if result and normalize_config:
        try:
            normalize_config["record_index"] = normalize_config.get("record_index", 0)
            result = normalize_streaming_record(result, normalize_config)
            if log_fn:
                log_fn.info(f"[NORMALIZE] Stub row normalized.")
        except Exception as e:
            if log_fn:
                log_fn.warning(f"[NORMALIZE] Failed to normalize stub row: {e}")
            else:
                print(f"[WARN] Failed to normalize stub row: {e}")

    return result if result else None


def get_stub_column_config(header_fields, normalize_config=None, log_fn=None):
    """
    Prompts user for new stub column name and logic type.
    Supports:
        - Fixed value
        - Text-based condition
        - Numeric comparison
    """
    config = {}

    col_name = simpledialog.askstring("Stub Column", "Enter name for new stub column:")
    if not col_name:
        messagebox.showerror("Missing Name", "No column name entered.")
        return None
    config["new_column"] = col_name

    logic_type = simpledialog.askstring(
        "Stub Logic Type",
        "Select logic type:\n\n"
        "1 - Fixed value\n"
        "2 - Text-based condition (e.g., if column contains X)\n"
        "3 - Numeric condition (e.g., if column > X)"
    )
    if not logic_type:
        messagebox.showerror("Cancelled", "No logic selected.")
        return None

    if logic_type == "1":
        fixed_value = simpledialog.askstring("Fixed Value", f"Enter value to insert in column '{col_name}':")
        if fixed_value is None:
            return None
        config["value"] = fixed_value

    elif logic_type == "2":
        col_index = gui_select_field("Select Field to Examine (Text Match)", header_fields)
        if col_index is None:
            return None
        try:
            keyword = simpledialog.askstring("Match Text", "Insert value only if field CONTAINS this text:")
            if keyword is None:
                return None
            value_if_match = simpledialog.askstring("Stub Value", "Enter value to insert if match occurs:")
            if value_if_match is None:
                return None
            config.update({
                "condition_type": "text",
                "source_col": col_index,
                "match_text": keyword,
                "value": value_if_match
            })
        except Exception as e:
            messagebox.showerror("Invalid Input", f"Error: {e}")
            return None

    elif logic_type == "3":
        col_index = gui_select_field("Select Field to Examine (Numeric Condition)", header_fields)
        if col_index is None:
            return None
        try:
            operator = simpledialog.askstring("Operator", "Enter comparison (>, <, >=, <=, ==):")
            if operator is None:
                return None
            threshold = simpledialog.askstring("Threshold", "Enter numeric value to compare against:")
            if threshold is None:
                return None
            value_if_match = simpledialog.askstring("Stub Value", "Enter value to insert if condition is true:")
            if value_if_match is None:
                return None
            config.update({
                "condition_type": "numeric",
                "source_col": col_index,
                "operator": operator,
                "threshold": float(threshold),
                "value": value_if_match
            })
        except Exception as e:
            messagebox.showerror("Invalid Input", f"Error: {e}")
            return None

    else:
        messagebox.showerror("Invalid Option", "Please enter 1, 2, or 3.")
        return None

    return config
