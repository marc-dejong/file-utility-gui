# dedupe_ui_8x.py
import tkinter as tk
from tkinter import messagebox

def prompt_dedupe_mode():
    mode_selected = {"mode": None}

    def on_select(mode):
        mode_selected["mode"] = mode
        win.destroy()

    win = tk.Toplevel()
    win.title("Select Dedupe Mode")
    win.geometry("400x180")
    win.grab_set()  # Modal

    tk.Label(win, text="Choose deduplication mode:", pady=10).pack()

    tk.Button(win, text="Keep Unique Records (remove duplicates)",
              width=40, command=lambda: on_select("dedupe")).pack(pady=5)

    tk.Button(win, text="Keep Duplicates Only (remove unique)",
              width=40, command=lambda: on_select("keep_duplicates_only")).pack(pady=5)

    tk.Button(win, text="Cancel", width=20,
              command=lambda: on_select(None)).pack(pady=10)

    win.wait_window()

    return mode_selected["mode"]

def select_dedupe_mode(shared_data=None):
    """
    Return one of: "dedupe", "keep_duplicates_only", or None.
    If shared_data['dedupe_mode'] is already set, return it without opening a dialog.
    """
    # Return cached selection if already chosen in this run
    if shared_data and shared_data.get("dedupe_mode") in ("dedupe", "keep_duplicates_only"):
        return shared_data["dedupe_mode"]

    # Otherwise, show the dialog once
    mode = prompt_dedupe_mode()

    # Cache it so repeated calls won't re-open the dialog
    if shared_data is not None and mode in ("dedupe", "keep_duplicates_only"):
        shared_data["dedupe_mode"] = mode

    return mode


# --- NEW: pick columns to dedupe on ---
import tkinter as tk
from tkinter import messagebox

def prompt_dedupe_fields(header_fields, title="Pick columns to dedupe on"):
    """
    Modal listbox to choose one or more columns for dedupe.
    Returns a list of selected column names, or None if canceled.
    """
    # --- Canonicalize header_fields into a list of column names ---
    # Handle cases where a single semicolon/comma/tab-delimited string was passed.
    if isinstance(header_fields, str):
        raw = header_fields.strip()
        sep = ';' if ';' in raw else (',' if ',' in raw else ('\t' if '\t' in raw else None))
        header_fields = [c.strip() for c in (raw.split(sep) if sep else [raw]) if c.strip()]
    elif (
        isinstance(header_fields, (list, tuple))
        and len(header_fields) == 1
        and isinstance(header_fields[0], str)
        and any(sep in header_fields[0] for sep in (';', ',', '\t'))
    ):
        raw = header_fields[0]
        sep = ';' if ';' in raw else (',' if ',' in raw else '\t')
        header_fields = [c.strip() for c in raw.split(sep) if c.strip()]

    selection = {"fields": None}

    def on_ok():
        sel_idx = listbox.curselection()
        if not sel_idx:
            messagebox.showwarning("No columns selected", "Pick at least one column.")
            return
        selection["fields"] = [header_fields[i] for i in sel_idx]
        win.destroy()

    def on_cancel():
        selection["fields"] = None
        win.destroy()

    def on_select_all():
        listbox.select_set(0, tk.END)

    def on_clear_all():
        listbox.selection_clear(0, tk.END)

    win = tk.Toplevel()
    win.title(title)
    win.geometry("420x400")
    win.grab_set()  # Modal

    tk.Label(win, text="Select one or more columns:", pady=8).pack()

    frame = tk.Frame(win)
    frame.pack(fill="both", expand=True, padx=10, pady=4)

    scrollbar = tk.Scrollbar(frame)
    scrollbar.pack(side="right", fill="y")

    listbox = tk.Listbox(frame, selectmode=tk.MULTIPLE, exportselection=False)
    listbox.pack(side="left", fill="both", expand=True)
    listbox.config(yscrollcommand=scrollbar.set)
    scrollbar.config(command=listbox.yview)

    for i, col in enumerate(header_fields):
        listbox.insert(tk.END, f"{i+1:>2}. {col}")

    btns = tk.Frame(win)
    btns.pack(pady=10)
    tk.Button(btns, text="Select All", width=12, command=on_select_all).grid(row=0, column=0, padx=5)
    tk.Button(btns, text="Clear All",  width=12, command=on_clear_all).grid(row=0, column=1, padx=5)
    tk.Button(btns, text="OK",         width=12, command=on_ok).grid(row=0, column=2, padx=5)
    tk.Button(btns, text="Cancel",     width=12, command=on_cancel).grid(row=0, column=3, padx=5)

    win.wait_window()
    return selection["fields"]
