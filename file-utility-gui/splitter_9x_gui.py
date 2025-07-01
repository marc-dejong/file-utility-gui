# splitter_9x_gui.py
# GUI-based split by condition module (Dask-compatible)

import os
import tkinter as tk
from tkinter import simpledialog, messagebox
import pandas as pd
import dask.dataframe as dd
import csv

def split_file_by_condition(shared_data):
    delimiter = shared_data["delimiter"]
    input_file = shared_data["input_file"]

    # Prompt user for split field
    field = gui_select_field("Select field to split on", shared_data["header"])
    if not field:
        messagebox.showwarning("Cancelled", "No field selected for splitting.")
        return

    # Prompt user for value(s) to split on
    value_input = simpledialog.askstring(
        "Split Value(s)",
        f"Enter one or more match values for field '{field}' (comma-separated):"
    )
    if not value_input:
        messagebox.showwarning("Cancelled", "No match values entered.")
        return

    # Parse and normalize input values
    values = [v.strip().lower() for v in value_input.split(",") if v.strip()]
    if not values:
        messagebox.showerror("Invalid Input", "No valid match values found.")
        return

    # Load data
    df = dd.read_csv(
        input_file,
        assume_missing=True,
        encoding="utf-8-sig",
        dtype=str,
        sep=delimiter,
        blocksize="64MB"
    )

    # Normalize field values (trim + lower)
    df[field] = df[field].astype( str ).str.strip().str.lower()

    # Perform split
    match_df = df[df[field].isin( values )]
    nonmatch_df = df[~df[field].isin( values )]

    # Prepare output paths
    base, ext = os.path.splitext( input_file )
    match_file = f"{base}._MATCH{ext}"
    nonmatch_file = f"{base}._NOMATCH{ext}"

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

    print(f"[9x_Splitter] Split complete:")
    print(f"  Match → {match_file}")
    print(f"  Non-match → {nonmatch_file}")

def gui_select_field(title, header_list):
    win = tk.Toplevel()
    win.title(title)
    win.geometry("400x300")
    tk.Label(win, text="Select one field:").pack(pady=(10, 5))

    selected = []

    listbox = tk.Listbox(win, selectmode=tk.SINGLE, exportselection=False)
    for idx, name in enumerate(header_list):
        listbox.insert(tk.END, f"{idx}: {name}")
    listbox.pack(fill=tk.BOTH, expand=True, padx=10)

    def on_submit():
        selection = listbox.curselection()
        if not selection:
            messagebox.showerror("No Selection", "Please select a field.")
            return
        raw = listbox.get(selection[0])
        name = raw.split(": ", 1)[1]
        selected.append(name)
        win.destroy()

    tk.Button(win, text="OK", command=on_submit).pack(pady=10)
    win.wait_window()

    return selected[0] if selected else None
