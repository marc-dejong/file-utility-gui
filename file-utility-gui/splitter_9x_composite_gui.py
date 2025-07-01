# splitter_9x_composite_gui.py
# GUI-based split by composite condition (multi-column matching)

import os
import tkinter as tk
from tkinter import simpledialog, messagebox
import pandas as pd
import dask.dataframe as dd
import csv


def split_by_composite_condition(shared_data):
    delimiter = shared_data["delimiter"]
    input_file = shared_data["input_file"]
    header = shared_data["header"]

    # Prompt user to select multiple fields
    fields = gui_select_multiple_fields("Select fields for composite split", header)
    if not fields:
        messagebox.showwarning("Cancelled", "No fields selected.")
        return

    # Prompt for matching value for each field
    match_values = {}
    for field in fields:
        val = simpledialog.askstring("Match Value", f"Enter match value for '{field}':")
        if val is None:
            messagebox.showwarning("Cancelled", f"No value provided for '{field}'.")
            return
        match_values[field] = val.strip().lower()

    # Load data
    df = dd.read_csv(
        input_file,
        encoding="utf-8-sig",
        assume_missing=True,
        dtype=str,
        sep=delimiter,
        blocksize="64MB"
    )

    # Normalize and filter for match
    for field, match in match_values.items():
        df = df[df[field].astype(str).str.strip().str.lower() == match]

    match_df = df

    # Load full data for nonmatch
    full_df = dd.read_csv(
        input_file,
        encoding="utf-8-sig",
        assume_missing=True,
        dtype=str,
        sep=delimiter,
        blocksize="64MB"
    )

    nonmatch_df = full_df
    for field, match in match_values.items():
        nonmatch_df = nonmatch_df[nonmatch_df[field].astype(str).str.strip().str.lower() != match]

    # Prepare output paths
    base, ext = os.path.splitext(input_file)
    first_field = fields[0].replace(" ", "_")
    match_file = f"{base}._MATCH_{first_field}{ext}"
    nonmatch_file = f"{base}._NOMATCH_{first_field}{ext}"

    # Determine quoting behavior based on GUI checkbox
    strip_quotes = shared_data["flags"].get( "strip_quotes", False )

    if strip_quotes:
        quoting = csv.QUOTE_NONE
        quotechar = ''
        escapechar = '\\'
    else:
        quoting = csv.QUOTE_NONNUMERIC
        quotechar = '"'
        escapechar = None

    # Write match records
    match_df.to_csv(
        match_file,
        index=False,
        single_file=True,
        sep=delimiter,
        quoting=quoting,
        quotechar=quotechar,
        escapechar=escapechar
    )

    # Write non-match records
    nonmatch_df.to_csv(
        nonmatch_file,
        index=False,
        single_file=True,
        sep=delimiter,
        quoting=quoting,
        quotechar=quotechar,
        escapechar=escapechar
    )

    print("[9x_Splitter_Composite] Split complete:")
    print(f"  Match → {match_file}")
    print(f"  Non-match → {nonmatch_file}")

def gui_select_multiple_fields(title, header_list):
    win = tk.Toplevel()
    win.title(title)

    visible_height = min(len(header_list), 20)
    win.geometry(f"400x{(visible_height * 20) + 120}")

    tk.Label(win, text="Select multiple fields:").pack(pady=(10, 5))

    list_frame = tk.Frame(win)
    list_frame.pack(fill=tk.BOTH, expand=True, padx=10)

    scrollbar = tk.Scrollbar(list_frame)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    listbox = tk.Listbox(list_frame, selectmode=tk.MULTIPLE, exportselection=False, yscrollcommand=scrollbar.set)
    for idx, name in enumerate(header_list):
        listbox.insert(tk.END, f"{idx}: {name}")

    listbox.config(height=visible_height)
    listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.config(command=listbox.yview)

    selected = []

    def on_submit():
        selections = listbox.curselection()
        if not selections:
            messagebox.showerror("No Selection", "Please select at least one field.")
            return
        for i in selections:
            raw = listbox.get(i)
            name = raw.split(": ", 1)[1]
            selected.append(name)
        win.destroy()

    tk.Button(win, text="OK", command=on_submit).pack(pady=10)
    win.wait_window()

    return selected
