"""
Program Name: 0x_File_Util_Caller.py
Date Created: 2025-04-02
Last Modified: 2025-04-02

Description:
Master controller module for the Multi-Function File Utility suite.
Initializes environment variables, sets up shared interface record, and routes execution flow
based on user interaction. Acts as the central traffic controller.

Parameters Sent:
- shared_data (dict): Interface record passed to all modules.

Parameters Received:
- Output from user input and functional modules via shared_data.

Return Values:
- Exit status or summary message printed to console.

Dependencies:
- 1x_User_Interface.py
- 2x_File_Loader.py
- All functional modules (3x_ to 10x_)
- 98x_Logger.py
- 99x_Error_Handling.py

Notes / TODOs:
- Implement actual module routing logic
- Connect logger and error handler as needed
- Stub out calls to downstream modules

╔══════════════════════════════════════════════════════════════════════╗
║ Dynamic Module Calls Summary                                         ║
║                                                                      ║
║ The following external modules are dynamically loaded at runtime:    ║
║                                                                      ║
║ - 1x_User_Interface.py  → User input and function selection          ║
║   └─ user_interface.capture_user_inputs_gui()                            ║
║                                                                      ║
║ - 2x_File_Loader.py     → File reading and writing (CSV/TXT)         ║
║   └─ shared_data["results"]["input_data"] = file_loader.read_file(..)║
║   └─ records_out = file_loader.write_file(...)                       ║
║                                                                      ║
║ - 3x_Filters.py         → Filtering logic (filter_select)            ║
║   └─ filters.apply_filters(row, rules, flags, header)                ║
║                                                                      ║
║                                                                      ║
║ These modules must reside in the same directory as this script.      ║
║ All calls use importlib.util to allow flexible runtime execution.    ║
║ This list should be updated as additional dynamic modules are added. ║
╚══════════════════════════════════════════════════════════════════════╝

Notes / TODOs:
- Implement actual module routing logic
- Connect logger and error handler as needed
- Stub out calls to downstream modules

Change Log:
- 2025-04-02: Initial scaffold
- 2025-04-02: Aligned with one-pass function input structure
- 2025-04-02: Added header detection prompt and flag
- 2025-04-02: Added filter criteria prompts for functions 1 and 2
- 2025-04-02: Added case sensitivity and quote stripping flags
- 2025-04-02: Added restart option and in-field cancel for filtering rules
- 2025-04-02: Moved file loading before filter rule input to enable field name selection
- 2025-05-22: Clean up and mod dedupe to add (optional) RESULTS_DUPLICATES file
- 2025-05-27: Major rewrite to GUI version with GUI widgets
"""

import os
import sys
import traceback
import csv
import importlib.util
import warnings
from pickle import FALSE

import pandas as pd
import subprocess
# from filters_ui_3x import collect_filter_rules
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from dask.config import config

from stub_5x_ui_config_gui import get_stub_column_config
from stub_5x_ui_gui import add_stub_column
import chardet  # Required if not already imported
from utils_11x_gui import safe_open


# Optional: Enable Dask if available
try:
    import dask
    import dask.dataframe as dd
    warnings.filterwarnings("ignore", module="dask")
    warnings.filterwarnings("ignore", message=".*dtype.*")
    warnings.filterwarnings("ignore", message=".*Assuming.*")
    warnings.filterwarnings("ignore", message=".*blocksize.*")
except ImportError:
    dd = None
    print( "[WARN] Dask is not installed. Advanced Mode will not be available." )

# UI and helper imports — delayed or dynamic imports still allowed later if needed
# You may comment this in/out as you re-integrate GUI versions
# from filters_ui_3x_gui import collect_filter_rules

# Optional: Use Dask distributed client for performance tuning
# from dask.distributed import Client
# client = Client(
#     memory_limit="2GB",  # Per worker memory cap
#     n_workers=4          # Number of concurrent worker threads
# )
# print(f"[INFO] Dask distributed client started: {client}")

# Preload file_loader so we can use detect_delimiter early
file_loader_spec = importlib.util.spec_from_file_location("file_loader", "./2x_File_Loader_gui.py")
file_loader = importlib.util.module_from_spec(file_loader_spec)
file_loader_spec.loader.exec_module(file_loader)

# Initialize shared interface dictionary
def create_shared_data():
    return {
        "function_queue": [],
        "input_file": None,
        "output_file": None,
        "filter_terms": [],
        "replacement": {},
        "split_conditions": [],
        "concatenate_files": [],
        "flags": {
            "debug": False,                # SET TO TRUE FOR DASK TUNING THEN TRY RE-RUNNING
            "dry_run": False,
            "case_sensitive": False,
            "strip_quotes": False,
            "flexible_decoding": False
        },
        "has_header": False,
        "header": [],
        "filter_rules": [],
        "results": {},
        "errors": [],
        "chunk_size": 1
    }

# Shared prompt for selecting one or more column fields
def prompt_for_field_list(action_type, shared_data):
    """
    Prompts the user to select one or more columns for sorting, deduping, or splitting.
    Returns a validated list of field names or indices (as entered).
    """
    print(f"\n[INPUT] Please select column(s) to use for '{action_type}' operation:")

    if shared_data["has_header"] and shared_data["header"]:
        print("\nAvailable Fields:")
        for idx, name in enumerate(shared_data["header"]):
            print(f"  {idx}: {name}")
        raw_input = input(f"\nEnter one or more field numbers (comma-separated) for {action_type}: ").strip()
    else:
        print("\n[WARN] No header detected — column numbers only (0-based)")
        raw_input = input(f"Enter one or more column numbers (comma-separated) for {action_type}: ").strip()

    field_list = [f.strip() for f in raw_input.split(",") if f.strip()]
    validated_fields = []

    for item in field_list:
        if item.isdigit():
            idx = int(item)
            if shared_data["has_header"]:
                if idx < 0 or idx >= len(shared_data["header"]):
                    print(f"[ERROR] Column index {idx} is out of range.")
                    return []
                validated_fields.append(shared_data["header"][idx])
            else:
                validated_fields.append(idx)
        else:
            if not shared_data["has_header"]:
                print(f"[ERROR] You must use numeric indices (no header available): '{item}' is invalid.")
                return []
            if item not in shared_data["header"]:
                print(f"[ERROR] Field name '{item}' not found in header.")
                return []
            validated_fields.append(item)

    print(f"\n[INFO] Final {action_type} field list (in order): {validated_fields}")
    confirm = input("Proceed with this list? (y/n): ").strip().lower()
    if confirm != "y":
        print("[INFO] Operation cancelled by user.")
        return []

    return validated_fields

def gui_select_fields(title, header_list):
    win = tk.Toplevel()
    win.title(title)
    win.geometry("400x300")
    tk.Label( win, text="Click fields in sort priority order (hold Ctrl to select):" )

    selected_fields_order = []

    indexed_fields = [f"{i}: {name}" for i, name in enumerate(header_list)]
    listbox = tk.Listbox(win, selectmode=tk.EXTENDED, exportselection=False)
    for item in indexed_fields:
        listbox.insert(tk.END, item)
    listbox.pack(fill=tk.BOTH, expand=True, padx=10)

    def on_select_field(event):
        widget = event.widget
        selections = widget.curselection()
        for idx in selections:
            try:
                field_line = widget.get(idx)
                field_name = field_line.split(": ", 1)[1]
                if field_name not in selected_fields_order:
                    selected_fields_order.append(field_name)
            except IndexError:
                continue

    listbox.bind("<<ListboxSelect>>", on_select_field)

    def on_submit():
        if not selected_fields_order:
            messagebox.showerror("No Selection", "Please select at least one field.")
            return
        win.destroy()

    tk.Button(win, text="OK", command=on_submit).pack(pady=10)
    win.wait_window()

    return selected_fields_order

class FileUtilityApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Multi-Function File Utility (GUI Version)")
        self.shared_data = self.create_shared_data()
        self.shared_data["root"] = self.root
        self.setup_main_frame()

    def create_shared_data(self):
        return {
            "function_queue": [],
            "input_file": None,
            "output_file": None,
            "filter_terms": [],
            "replacement": {},
            "split_conditions": [],
            "concatenate_files": [],
            "flags": {
                "debug": False,
                "dry_run": False,
                "case_sensitive": False,
                "strip_quotes": False,
                "flexible_decoding": False
            },
            "has_header": False,
            "header": [],
            "filter_rules": [],
            "results": {},
            "errors": [],
            "chunk_size": 1
        }

    def gui_select_fields(title, header_list):
        win = tk.Toplevel()
        win.title( title )
        win.geometry( "450x500" )
        tk.Label( win, text="Select field(s) from list OR enter manually below:" ).pack( pady=(10, 5) )

        # Build listbox with "index: name"
        selected_fields_order = []
        indexed_fields = [f"{i}: {name}" for i, name in enumerate( header_list )]
        listbox = tk.Listbox( win, selectmode=tk.MULTIPLE, exportselection=False, height=15 )
        listbox.bind( "<<ListboxSelect>>", on_select_field )

        def on_select_field(event):
            widget = event.widget
            selections = widget.curselection()
            for idx in selections:
                try:
                    field_line = widget.get( idx )
                    field_name = field_line.split( ": ", 1 )[1]
                    if field_name not in selected_fields_order:
                        selected_fields_order.append( field_name )
                except IndexError:
                    continue

        for item in indexed_fields:
            listbox.insert( tk.END, item )
        listbox.pack( fill=tk.BOTH, expand=False, padx=10 )

        # Manual entry box
        tk.Label( win, text="...or enter column numbers or names (comma-separated):" ).pack( pady=(10, 5) )
        manual_entry = tk.Entry( win, width=50 )
        manual_entry.pack( pady=(0, 10) )

        selected = []

        def on_submit():
            selected_items = listbox.curselection()
            raw_input = manual_entry.get().strip()

            # From listbox
            for idx in selected_items:
                raw = indexed_fields[idx]
                col_name = raw.split( ": ", 1 )[1]
                selected.append( col_name )

            # From manual entry
            if raw_input:
                items = [item.strip() for item in raw_input.split( "," ) if item.strip()]
                for item in items:
                    if item.isdigit():
                        idx = int( item )
                        if 0 <= idx < len( header_list ):
                            selected.append( header_list[idx] )
                        else:
                            messagebox.showerror( "Invalid Index", f"Index {idx} is out of range." )
                            selected.clear()
                            return
                    elif item in header_list:
                        selected.append( item )
                    else:
                        messagebox.showerror( "Invalid Field", f"'{item}' is not a valid field name." )
                        selected.clear()
                        return

            if not selected:
                messagebox.showwarning( "No Selection", "Please select or enter at least one field." )
                return

            win.destroy()

        tk.Button( win, text="OK", command=on_submit ).pack( pady=10 )
        win.wait_window()

        return selected

    def prompt_for_key_fields(self):
        from tkinter import Toplevel, Listbox, Label, Button, MULTIPLE, EXTENDED, messagebox

        popup = Toplevel( self.root )  # Make this a true child of the main window
        popup.title( "Select Matching Key Fields from Key File" )

        # Size and position the window
        w = 400
        h = 300
        x = 600
        y = 200
        popup.geometry( f"{w}x{h}+{x}+{y}" )

        # Enforce focus and modal behavior
        popup.transient( self.root )  # Link to main window
        popup.grab_set()  # Block interactions with other windows
        popup.lift()  # Bring to front
        popup.attributes( "-topmost", True )  # Force top
        popup.after_idle( popup.attributes, "-topmost", False )
        popup.focus_force()

        Label(popup, text="Select one or more key fields from the main input file:").pack(pady=10)

        field_list = Listbox(popup, selectmode=EXTENDED, exportselection=False)
        field_list.pack(expand=True, fill="both", padx=10)

        input_headers = self.shared_data.get("header_fields", [])

        if not input_headers:
            input_file = self.shared_data.get( "input_file" )
            delimiter = self.shared_data.get( "delimiter", "," )
            try:
                f, enc_used, fallback = safe_open(
                    input_file,
                    mode="r",
                    newline="",
                    flexible=config["flags"].get( "flexible_decoding", False )
                )

                if fallback:
                    print( f"[INFO] Opened input file using fallback encoding: {enc_used}" )
                else:
                    print( f"[INFO] Opened input file using preferred encoding: {enc_used}" )
                with f:
                    reader = csv.reader( f, delimiter=delimiter )
                    first_row = next( reader, [] )
                    input_headers = [f"Field {i + 1}" for i in range( len( first_row ) )]
            except Exception as e:
                messagebox.showerror( "Header Load Error", f"Could not determine header fields:\n{e}" )
                popup.destroy()
                return

        # Populate the listbox
        # Populate the listbox with indexed labels
        for i, field in enumerate( input_headers ):
            label = f"{i} – {field}"
            field_list.insert( "end", label )

        def prompt_for_keyfile_fields(key_file_path, field_count):
            from tkinter import Toplevel, Listbox, Label, Button, EXTENDED, messagebox
            import csv

            popup = Toplevel()
            popup.title( "Select Matching Key Fields from Key File" )
            popup.geometry( "400x300" )

            Label( popup, text="Select the matching key fields from the key file (same number and order):" ).pack(
                pady=10 )

            listbox = Listbox( popup, selectmode=EXTENDED, exportselection=False )
            listbox.pack( expand=True, fill="both", padx=10 )

            # Load headers or fallback field names from key file
            try:
                delimiter = self.shared_data.get( "delimiter", "," )
                with open( key_file_path, "r", encoding="utf-8" ) as f:
                    reader = csv.reader( f, delimiter=delimiter )
                    first_row = next( reader, [] )
                if self.shared_data.get( "has_header" ):
                    headers = first_row
                else:
                    headers = [f"Field {i + 1}" for i in range( len( first_row ) )]
            except Exception as e:
                messagebox.showerror( "Header Load Error", f"Could not read key file:\n{e}" )
                popup.destroy()
                return

            for i, name in enumerate( headers ):
                label = f"{i} – {name}"
                listbox.insert( "end", label )

            def submit_mapping():
                selected = list( listbox.curselection() )
                if len( selected ) != field_count:
                    messagebox.showerror(
                        "Mismatch",
                        f"You must select exactly {field_count} field(s) to match the input file."
                    )
                    return

                self.shared_data["keyfile_key_fields"] = selected

                success = False
                try:
                    self.execute_merge_by_key()
                    success = True
                except Exception as e:
                    messagebox.showerror( "Merge-by-Key Error", f"Merge failed:\n{e}" )
                finally:
                    try:
                        popup.destroy()
                    except:
                        pass

                # ✅ Only show error if the merge didn’t happen
                if not success:
                    if not self.shared_data.get( "merge_successful" ):
                        messagebox.showwarning( "Missing File", "No key file was selected. Merge cannot continue." )

            Button( popup, text="OK", command=submit_mapping ).pack( pady=10 )

        def on_submit_keys():
            selected_indices = list(field_list.curselection())
            if not selected_indices:
                messagebox.showerror("Key Field Selection", "You must select at least one key field.")
                return
            self.shared_data["key_fields"] = selected_indices
            # Prompt for second file (key file)
            from tkinter import filedialog

            key_path = filedialog.askopenfilename(
                title="Select Key File",
                filetypes=[("CSV/TXT Files", "*.csv *.txt"), ("All Files", "*.*")]
            )
            if not key_path:
                if not self.shared_data.get( "merge_successful" ):
                    messagebox.showwarning( "Missing File", "No key file was selected. Merge cannot continue." )
                return

            self.shared_data["second_file"] = key_path

            # Now prompt for key field mapping from the key file
            self.root.after( 100, lambda: prompt_for_keyfile_fields( key_path, len( selected_indices ) ) )

            popup.destroy()
            try:
                # Prompt user to select the key file
                from tkinter import filedialog

                key_path = filedialog.askopenfilename(
                    title="Select Key File",
                    filetypes=[("CSV/TXT Files", "*.csv *.txt"), ("All Files", "*.*")]
                )
                if not key_path:
                    if not self.shared_data.get( "merge_successful" ):
                        messagebox.showwarning( "Missing File", "No key file was selected. Merge cannot continue." )
                    return

                self.shared_data["second_file"] = key_path

                self.execute_merge_by_key()

            except Exception as e:
                messagebox.showerror("Merge-by-Key Error", f"Merge by key failed: {e}")

        Button(popup, text="OK", command=on_submit_keys).pack(pady=10)


    def setup_main_frame(self):
        frame = ttk.Frame(self.root, padding="10")
        frame.grid(row=0, column=0, sticky="nsew")

        ttk.Label(frame, text="Step 1: Load Input File").grid(row=0, column=0, sticky="w")

        self.load_btn = ttk.Button(frame, text="Select File", command=self.load_input_file)
        self.load_btn.grid(row=1, column=0, pady=5, sticky="w")

        self.file_label = ttk.Label(frame, text="No file selected.")
        self.file_label.grid(row=2, column=0, sticky="w")

        # Header record checkbox
        self.has_header_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Input file contains a header record",
            variable=self.has_header_var
        ).grid( row=3, column=0, pady=(10, 0), sticky="w" )

        # Case sensitivity checkbox
        self.case_sensitive_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Enable case-sensitive filtering",
            variable=self.case_sensitive_var
        ).grid( row=4, column=0, sticky="w" )

        # Quote stripping checkbox
        self.strip_quotes_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Strip leading/trailing quotes from fields",
            variable=self.strip_quotes_var
        ).grid( row=5, column=0, sticky="w" )

        # Flexible decoding checkbox
        self.flexible_decoding_var = tk.BooleanVar( value=False )
        ttk.Checkbutton(
            frame,
            text="Enable flexible encoding fallback (try CP1252, Latin-1 if UTF-8 fails)",
            variable=self.flexible_decoding_var
        ).grid( row=6, column=0, sticky="w" )

        # Function selection label
        ttk.Label( frame, text="Step 2: Select Functions to Apply" ).grid( row=7, column=0, pady=(15, 0), sticky="w" )

        self.function_listbox = tk.Listbox( frame, selectmode=tk.MULTIPLE, height=10, exportselection=False )
        self.function_listbox.grid( row=7, column=0, sticky="w", pady=5 )

        # GUI-only: Function menu for GUI File Utility
        self.function_options = {
            "1": "filter_omit",
            "2": "filter_select",
            "3": "replace_rec_contents",
            "4": "add_rec_stub_(fixed)",
            "5": "add_rec_stub_(var_from_rec_contents)",
            "6": "delete_rec_by_condition",
            "7": "sort_records",
            "8": "dedupe_records",
            "9": "split_file_by_condition",
            "10": "split_by_composite_condition",  # ✅ INSERT HERE
            "11": "concatenate_files",
            "12": "merge_by_key",
            "13": "file_compare"
        }

        for key in sorted( self.function_options.keys(), key=lambda x: int( x ) ):
            label = f"{key}. {self.function_options[key]}"
            self.function_listbox.insert( tk.END, label )

        # Continue button
        self.continue_btn = ttk.Button(
            frame,
            text="Continue",
            command=self.on_continue
        )
        self.continue_btn.grid( row=8, column=0, pady=(15, 0), sticky="w" )

    def load_input_file(self):
        filepath = filedialog.askopenfilename(
            title="Select Input File",
            filetypes=[("CSV/TXT Files", "*.csv *.txt"), ("All Files", "*.*")]
        )
        if filepath:
            self.shared_data["input_file"] = filepath
            self.file_label.config(text=os.path.basename(filepath))
            messagebox.showinfo("File Selected", f"Loaded file:\n{filepath}")
        else:
            self.file_label.config(text="No file selected.")

    def capture_function_selection(self):
        selected_indices = self.function_listbox.curselection()
        self.shared_data["function_queue"] = [
            self.function_options[str(i + 1)]
            for i in selected_indices
            if str(i + 1) in self.function_options
        ]

    def execute_merge_by_key(self):
        from merger_gui_10x import run_merge_10x_process
        run_merge_10x_process( self.shared_data, mode="key" )

    def on_continue(self):
        self.capture_function_selection()

        # Save checkbox flags
        self.shared_data["has_header"] = self.has_header_var.get()
        self.shared_data["flags"]["case_sensitive"] = self.case_sensitive_var.get()
        self.shared_data["flags"]["strip_quotes"] = self.strip_quotes_var.get()
        self.shared_data["flags"]["flexible_decoding"] = self.flexible_decoding_var.get()

        # Basic validation
        selected_queue = self.shared_data.get( "function_queue", [] )
        if "file_compare" not in selected_queue and not self.shared_data["input_file"]:
            messagebox.showerror( "Missing Input", "Please select an input file before continuing." )
            return

        # Skip input file check if file_compare is the only selected function
        selected_queue = self.shared_data.get( "function_queue", [] )
        if (
                "file_compare" not in selected_queue
                and not self.shared_data["input_file"]
        ):
            messagebox.showerror( "Missing Input", "Please select an input file before continuing." )
            return

        # Debug preview (can be replaced with actual routing later)
        print( "\n--- Summary ---" )
        print( "Input File      :", self.shared_data["input_file"] )
        print( "Header Present  :", self.shared_data["has_header"] )
        print( "Case Sensitive  :", self.shared_data["flags"]["case_sensitive"] )
        print( "Strip Quotes    :", self.shared_data["flags"]["strip_quotes"] )
        print( "Function Queue  :", self.shared_data["function_queue"] )
        print( "----------------" )

        messagebox.showinfo( "Ready", "Configuration complete. Ready to launch processing." )
        # 🚫 Skip all input file logic for file_compare
        if "file_compare" in self.shared_data["function_queue"]:
            subprocess.Popen( ["python", "Two_File_Compare_gui.py"] )
            return

        # Detect delimiter
        self.shared_data["delimiter"] = file_loader.detect_delimiter( self.shared_data["input_file"] )
        print( f"[INFO] Detected delimiter: '{self.shared_data['delimiter']}'" )

        # Set output file path
        if "file_compare" not in self.shared_data["function_queue"]:
            base, ext = os.path.splitext( self.shared_data["input_file"] )
        else:
            base, ext = "output", ".csv"

        output_path = f"{base}._RESULTS{ext}"
        self.shared_data["output_file"] = output_path
        print( f"[INFO] Output file will be: {output_path}" )

        # Try to load file header
        record_stream = file_loader.read_file(
            self.shared_data["input_file"],
            self.shared_data,
            self.shared_data["delimiter"]
        )

        try:
            # Always peek first row
            first_row = next( record_stream )

            # If user says header is present, parse it
            if self.shared_data["has_header"]:
                self.shared_data["header"] = first_row
                self.shared_data["header_fields"] = self.shared_data["header"]

            else:
                self.shared_data["header"] = [f"COL_{i}" for i in range( len( first_row ) )]
                record_stream = (row for row in [first_row] + list( record_stream ))

        except StopIteration:
            messagebox.showerror( "File Error", "The input file appears to be empty." )
            self.shared_data["errors"].append( "Empty input file." )
            return
        except Exception as e:
            messagebox.showerror( "Read Error", f"Failed to read file header: {e}" )
            self.shared_data["errors"].append( "Header priming failed." )
            return

        # Detect execution mode
        stream_functions = {
            "filter_omit", "filter_select", "replace_rec_contents",
            "add_rec_stub_(fixed)", "add_rec_stub_(var_from_rec_contents)",
            "delete_rec_by_condition", "concatenate_files"
        }

        dask_functions = {
            "sort_records",
            "dedupe_records",
            "split_file_by_condition",
            "split_by_composite_condition"
        }

        selected = set( self.shared_data["function_queue"] )
        uses_stream = any( f in stream_functions for f in selected )
        uses_dask = any( f in dask_functions for f in selected )

        if uses_stream and uses_dask:
            messagebox.showerror(
                "Invalid Function Mix",
                "You selected a mix of incompatible functions (stream + dask). Please separate them."
            )
            return

        # if selected_function == "file_compare":
        #     try:
        #         subprocess.Popen( ["python", "Two_File_Compare_gui.py"] )
        #         return
        #     except Exception as e:
        #         messagebox.showerror( "Error", f"Failed to launch compare tool:\n{e}" )
        #         return

        elif uses_stream:
            self.shared_data["mode"] = "stream"
            print( "[INFO] Detected: Standard Mode (stream-based, low memory)." )
        elif uses_dask:
            self.shared_data["mode"] = "dask"
            print("[INFO] Detected: Advanced Mode (full-file, parallelized).")

        elif "merge_by_key" in self.shared_data["function_queue"]:
            print("[EXECUTION] Routing to Menu 12: Merge by Key")
            try:
                self.prompt_for_key_fields()
                return
            except Exception as e:
                messagebox.showerror("Merge-by-Key Error", f"Merge by key failed: {e}")
                return

        elif "concatenate_files" in self.shared_data["function_queue"]:
            print( "[EXECUTION] Routing to Menu 11: Concatenate Files" )
            try:
                from merger_gui_10x import run_merge_10x_process
                run_merge_10x_process( self.shared_data, mode="basic" )
                return
            except Exception as e:
                messagebox.showerror( "Concatenate Error", f"Concatenation failed:\n{e}" )
                return


        else:
            messagebox.showerror(
                "Unknown Mode",
                "Could not determine execution mode based on selected functions."
            )
            return



        # Placeholder routing — will be replaced with actual logic

        mode = self.shared_data["mode"]

        if mode == "stream":
            print( "[EXECUTION] Routing to stream mode logic..." )
            try:
            # General stream-mode function router
                for function_name in self.shared_data["function_queue"]:
                    print( f"[ROUTER] Processing function: {function_name}" )

                    if function_name == "filter_select":
                        try:
                            from filters_ui_3x_gui import collect_filter_rules_gui
                            from filters_3x_gui import apply_filters

                            self.shared_data["filter_rules"] = collect_filter_rules_gui( self.shared_data["header"] )
                            if not self.shared_data["filter_rules"]:
                                messagebox.showinfo( "No Filters", "No filter rules provided. Skipping filtering step." )
                                continue
                            print( f"[INFO] Collected {len( self.shared_data['filter_rules'] )} rule(s)." )
                            print( "[INFO] Filter rule(s) received:" )
                            for idx, rule in enumerate( self.shared_data["filter_rules"], start=1 ):
                                print( f"  Rule {idx}: {rule}" )

                            writer, output_file = file_loader.write_file(
                                self.shared_data["output_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            if not writer:
                                messagebox.showerror( "Write Error", "Could not initialize output writer." )
                                return

                            # Re-read file stream to apply filters from the top
                            record_stream = file_loader.read_file(
                                self.shared_data["input_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            # Skip header row if needed
                            try:
                                first_row = next( record_stream )
                                if self.shared_data["has_header"]:
                                    if first_row == self.shared_data["header"]:
                                        print( "[DEBUG] Skipping repeated header row." )
                                    else:
                                        # If it's not identical, re-insert into stream
                                        record_stream = (row for row in [first_row] + list( record_stream ))
                                else:
                                    record_stream = (row for row in [first_row] + list( record_stream ))
                            except StopIteration:
                                messagebox.showerror( "Input Error", "No data rows found." )
                                return

                            # Process each row
                            count_in = 0
                            count_out = 0

                            header_written = False

                            for row in record_stream:
                                # Check if this row is the header row
                                if self.shared_data["has_header"] and row == self.shared_data["header"]:
                                    if not header_written:
                                        writer.writerow( self.shared_data["header"] )
                                        header_written = True
                                    else:
                                        print( "[DEBUG] Skipping repeated header row." )
                                    continue

                                # Apply filters to each row
                                count_in += 1
                                if apply_filters( row, self.shared_data["filter_rules"], self.shared_data,
                                                  header=self.shared_data["header"] ):
                                    writer.writerow( row )
                                    count_out += 1

                            output_file.close()

                            print( f"[INFO] Records processed : {count_in:,}" )
                            print( f"[INFO] Records matched   : {count_out:,}" )
                            messagebox.showinfo(
                                "Filter Complete",
                                f"Matched {count_out:,} record(s).\nOutput written to:\n{self.shared_data['output_file']}"
                            )

                            self.root.quit()
                            self.root.destroy()
                            sys.exit( 0 )

                        except Exception as e:
                            messagebox.showerror( "Filter Error", f"Failed during filter_select: {e}" )
                            self.shared_data["errors"].append( "filter_select failed." )
                            return

                    elif function_name == "filter_omit":
                        try:
                            from filters_ui_3x_gui import collect_filter_rules_gui
                            from filters_3x_gui import apply_filters

                            self.shared_data["filter_rules"] = collect_filter_rules_gui(self.shared_data["header"])
                            if not self.shared_data["filter_rules"]:
                                messagebox.showinfo("No Filters", "No filter rules provided. Skipping filtering step.")
                                continue
                            print(f"[INFO] Collected {len(self.shared_data['filter_rules'])} rule(s).")
                            print("[INFO] Filter rule(s) received:")
                            for idx, rule in enumerate(self.shared_data["filter_rules"], start=1):
                                print(f"  Rule {idx}: {rule}")

                            writer, output_file = file_loader.write_file(
                                self.shared_data["output_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            if not writer:
                                messagebox.showerror("Write Error", "Could not initialize output writer.")
                                return

                            # Re-read file stream to apply filters from the top
                            record_stream = file_loader.read_file(
                                self.shared_data["input_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            # Skip header row if needed
                            try:
                                first_row = next(record_stream)
                                if self.shared_data["has_header"]:
                                    if first_row == self.shared_data["header"]:
                                        print("[DEBUG] Skipping repeated header row.")
                                    else:
                                        record_stream = (row for row in [first_row] + list(record_stream))
                                else:
                                    record_stream = (row for row in [first_row] + list(record_stream))
                            except StopIteration:
                                messagebox.showerror("Input Error", "No data rows found.")
                                return

                            # Process each row
                            count_in = 0
                            count_out = 0
                            header_written = False

                            for row in record_stream:
                                # Check if this row is the header row
                                if self.shared_data["has_header"] and row == self.shared_data["header"]:
                                    if not header_written:
                                        writer.writerow(self.shared_data["header"])
                                        header_written = True
                                    else:
                                        print("[DEBUG] Skipping repeated header row.")
                                    continue

                                # Apply inverted filter: write only if NOT matching
                                count_in += 1
                                if not apply_filters(row, self.shared_data["filter_rules"], self.shared_data,
                                                     header=self.shared_data["header"]):
                                    writer.writerow(row)
                                    count_out += 1

                            output_file.close()

                            print( "\n=== FILTER OMIT SUMMARY ===" )
                            print(f"[INFO] Records processed : {count_in:,}")
                            print(f"[INFO] Records retained  : {count_out:,}")
                            messagebox.showinfo(
                                "Filter (Omit) Complete",
                                f"Retained {count_out:,} record(s).\nOutput written to:\n{self.shared_data['output_file']}"
                            )

                            self.root.quit()
                            self.root.destroy()
                            sys.exit(0)

                        except Exception as e:
                            messagebox.showerror("Filter Omit Error", f"Failed during filter_omit: {e}")
                            self.shared_data["errors"].append("filter_omit failed.")
                            return

                    elif function_name == "add_rec_stub_(fixed)":
                        try:
                            config = get_stub_column_config(
                                self.shared_data["header"],
                                self.shared_data["has_header"]
                            )

                            if not config:
                                messagebox.showinfo( "Stub Skipped", "No stub configuration provided." )
                                return

                            output_path = self.shared_data["output_file"]
                            input_file = self.shared_data["input_file"]
                            delimiter = self.shared_data["delimiter"]

                            try:
                                infile, enc_in, _ = safe_open(
                                    input_file,
                                    mode="r",
                                    newline="",
                                    flexible=config["flags"].get( "flexible_decoding", True )
                                )

                                outfile, enc_out, _ = safe_open(
                                    output_path,
                                    mode="w",
                                    newline="",
                                    flexible=config["flags"].get( "flexible_decoding", True )
                                )

                                print( f"[INFO] Opened input file using encoding: {enc_in}" )
                                print( f"[INFO] Opened output file using encoding: {enc_out}" )
                            except Exception as e:
                                print( f"[ERROR] File open failed: {e}" )
                                messagebox.showerror( "File Open Error", f"Could not open file:\n{e}" )
                                return

                            with infile, outfile:
                                first_line = infile.readline()
                                if self.shared_data["has_header"]:
                                    header = first_line.strip().split( delimiter )
                                    header.append( config["new_column"] )
                                    outfile.write( delimiter.join( header ) + "\n" )
                                else:
                                    outfile.write( first_line )

                                for line in infile:
                                    row = line.strip().split( delimiter )
                                    new_row, _ = add_stub_column(
                                        row=row,
                                        header_fields=self.shared_data["header"],
                                        config=config
                                    )
                                    outfile.write( delimiter.join( new_row ) + "\n" )

                            print( "\n=== ADD FIXED STUB SUMMARY ===" )
                            print( f"[INFO] Stub column added   : {config['new_column']}" )
                            print( f"[INFO] Fixed value used    : {config['value']}" )
                            print( f"[INFO] Output file written : {output_path}" )

                            messagebox.showinfo(
                                "Stub Column Added",
                                f"New column '{config['new_column']}' added with fixed value.\nOutput written to:\n{output_path}"
                            )

                            self.root.quit()
                            self.root.destroy()
                            sys.exit( 0 )

                        except Exception as e:
                            print( f"[ERROR] Stub column addition failed: {e}" )
                            messagebox.showerror( "Stub Add Error", f"Failed to add stub column:\n{e}" )
                            self.shared_data["errors"].append( "add_rec_stub_(fixed) failed." )
                            return


                    elif function_name == "add_rec_stub_(var_from_rec_contents)":
                        try:
                            stub_ui_path = os.path.join( os.path.dirname( __file__ ), "5x_Add_Stub_UI_gui.py" )
                            spec_stub_ui = importlib.util.spec_from_file_location( "stub_ui", stub_ui_path )
                            stub_ui = importlib.util.module_from_spec( spec_stub_ui )
                            spec_stub_ui.loader.exec_module( stub_ui )
                            stub_logic_path = os.path.join( os.path.dirname( __file__ ), "5x_Add_Stub_gui.py" )
                            spec_stub_logic = importlib.util.spec_from_file_location( "stub_logic", stub_logic_path )
                            stub_logic = importlib.util.module_from_spec( spec_stub_logic )
                            spec_stub_logic.loader.exec_module( stub_logic )
                            config = stub_ui.get_stub_column_config(
                                self.shared_data["header"],
                                self.shared_data["has_header"]
                            )

                            if not config:
                                messagebox.showinfo(
                                    "Cancelled",
                                    "No stub configuration provided. Skipping this step."
                                )
                                continue

                            writer, output_file = file_loader.write_file(
                                self.shared_data["output_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            if not writer:
                                messagebox.showerror("Write Error", "Could not initialize writer.")
                                return

                            # Write header (with new column)
                            new_header = self.shared_data["header"] + [config["new_column"]]
                            writer.writerow(new_header)

                            # Re-read input file
                            record_stream = file_loader.read_file(
                                self.shared_data["input_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            try:
                                first_row = next(record_stream)
                                if self.shared_data["has_header"] and first_row == self.shared_data["header"]:
                                    pass  # already processed
                                else:
                                    record_stream = (row for row in [first_row] + list(record_stream))
                            except StopIteration:
                                messagebox.showerror("Input Error", "No data rows found.")
                                return

                            count = 0
                            for row in record_stream:
                                new_row, _ = stub_logic.add_stub_column(
                                    row=row,
                                    header_fields=self.shared_data["header"],
                                    config=config
                                )
                                writer.writerow(new_row)
                                count += 1

                            output_file.close()

                            print("\n=== ADD VARIABLE STUB SUMMARY ===")
                            print(f"[INFO] Stub column added: '{config['new_column']}'")
                            print(f"[INFO] Records processed: {count:,}")
                            messagebox.showinfo(
                                "Stub Column Added",
                                f"New column '{config['new_column']}' added to {count:,} records.\n"
                                f"Output written to:\n{self.shared_data['output_file']}"
                            )

                            self.root.quit()
                            self.root.destroy()
                            sys.exit(0)

                        except Exception as e:
                            messagebox.showerror("Stub (Derived) Error", f"Failed to add derived stub column: {e}")
                            self.shared_data["errors"].append("add_rec_stub_(var_from_rec_contents) failed.")
                            return

                    elif function_name == "delete_rec_by_condition":
                        try:
                            from filters_ui_3x_gui import collect_filter_rules_gui
                            from filters_3x_gui import apply_filters

                            self.shared_data["filter_rules"] = collect_filter_rules_gui(self.shared_data["header"])
                            if not self.shared_data["filter_rules"]:
                                messagebox.showinfo("No Filters", "No filter rules provided. Skipping delete step.")
                                continue
                            print(f"[INFO] Collected {len(self.shared_data['filter_rules'])} delete rule(s).")
                            for idx, rule in enumerate(self.shared_data["filter_rules"], start=1):
                                print(f"  Rule {idx}: {rule}")

                            writer, output_file = file_loader.write_file(
                                self.shared_data["output_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            if not writer:
                                messagebox.showerror("Write Error", "Could not initialize writer.")
                                return

                            # Re-read file
                            record_stream = file_loader.read_file(
                                self.shared_data["input_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            try:
                                first_row = next(record_stream)
                                if self.shared_data["has_header"] and first_row == self.shared_data["header"]:
                                    pass  # header already set
                                else:
                                    record_stream = (row for row in [first_row] + list(record_stream))
                            except StopIteration:
                                messagebox.showerror("Input Error", "No data rows found.")
                                return

                            count_in = 0
                            count_out = 0
                            count_deleted = 0
                            header_written = False

                            for row in record_stream:
                                if self.shared_data["has_header"] and row == self.shared_data["header"]:
                                    if not header_written:
                                        writer.writerow(self.shared_data["header"])
                                        header_written = True
                                    continue

                                count_in += 1
                                if apply_filters(row, self.shared_data["filter_rules"], self.shared_data,
                                                 header=self.shared_data["header"]):
                                    count_deleted += 1
                                    continue  # Skip row (i.e., delete)
                                writer.writerow(row)
                                count_out += 1

                            output_file.close()

                            print("\n=== DELETE BY CONDITION SUMMARY ===")
                            print(f"[INFO] Records processed : {count_in:,}")
                            print(f"[INFO] Records deleted   : {count_deleted:,}")
                            print(f"[INFO] Records retained  : {count_out:,}")
                            messagebox.showinfo(
                                "Delete Complete",
                                f"Deleted {count_deleted:,} record(s).\n"
                                f"Retained {count_out:,}.\n"
                                f"Output written to:\n{self.shared_data['output_file']}"
                            )

                            self.root.quit()
                            self.root.destroy()
                            sys.exit(0)

                        except Exception as e:
                            messagebox.showerror("Delete Error", f"Failed during delete step: {e}")
                            self.shared_data["errors"].append("delete_rec_by_condition failed.")
                            return

                    elif function_name == "replace_rec_contents":
                        try:
                            replacer_path = os.path.join( os.path.dirname( __file__ ), "4x_Replacer_gui.py" )
                            spec = importlib.util.spec_from_file_location( "replacer", replacer_path )
                            replacer = importlib.util.module_from_spec( spec )
                            spec.loader.exec_module( replacer )

                            replacer_ui_path = os.path.join( os.path.dirname( __file__ ), "4x_Replacer_UI_gui.py" )
                            spec_ui = importlib.util.spec_from_file_location( "replacer_ui", replacer_ui_path )
                            replacer_ui = importlib.util.module_from_spec( spec_ui )
                            spec_ui.loader.exec_module( replacer_ui )

                            # Get config from UI stub
                            # print( "[DEBUG] Header being passed to replacement config:", self.shared_data["header"] )

                            try:
                                self.shared_data["replace_config"] = replacer_ui.get_replacement_config_gui(
                                    self.shared_data["header"]
                                )
                                print( "[DEBUG] Replacement config received:", self.shared_data["replace_config"] )
                                if not self.shared_data["replace_config"].get( "rules" ):
                                    messagebox.showwarning(
                                        "Invalid Configuration",
                                        "No replacement rules were provided.\nProcess will not continue."
                                    )
                                    return

                                print( "[INFO] Replacement config loaded." )
                            except Exception as e:
                                print( f"[ERROR] Exception in replacement config dialog: {e}" )
                                traceback.print_exc()
                                messagebox.showerror( "Replacement Config Error", f"Exception: {e}" )
                                return

                            # simulate or call replacer.replace_rec_contents(...) on records
                            # Open writer and output file
                            writer, output_file = file_loader.write_file(
                                self.shared_data["output_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            if not writer:
                                messagebox.showerror( "Write Error", "Could not initialize writer." )
                                return

                            # Re-read file for streaming (since record_stream was already advanced)
                            record_stream = file_loader.read_file(
                                self.shared_data["input_file"],
                                self.shared_data,
                                self.shared_data["delimiter"]
                            )

                            # Skip header row if needed
                            try:
                                first_row = next( record_stream )
                                if not self.shared_data["has_header"]:
                                    record_stream = (row for row in [first_row] + list( record_stream ))
                            except StopIteration:
                                messagebox.showerror( "Input Error", "No data rows found." )
                                return

                            # Process and write
                            count_in = 0
                            count_out = 0
                            count_changed = 0

                            for row in record_stream:
                                count_in += 1
                                updated_row, was_changed = replacer.replace_rec_contents(
                                    row,
                                    self.shared_data["header"],
                                    self.shared_data["replace_config"]
                                )
                                if was_changed:
                                    count_changed += 1
                                writer.writerow( updated_row )
                                count_out += 1

                            output_file.close()

                            print( f"[INFO] Records processed : {count_in:,}" )
                            print( f"[INFO] Records written   : {count_out:,}" )
                            print( f"[INFO] Records modified  : {count_changed:,}" )
                            messagebox.showinfo( "Replacement Complete",
                                                 f"Modified {count_changed:,} record(s).\nOutput written to:\n{self.shared_data['output_file']}" )

                            # ✅ Exit the entire app cleanly
                            self.root.quit()
                            self.root.destroy()
                            sys.exit( 0 )

                        except Exception as e:
                            messagebox.showerror( "Replace Error", f"Failed during replace_rec_contents: {e}" )
                            self.shared_data["errors"].append( "replace_rec_contents failed." )
                            return


                    elif function_name == "concatenate_files":
                        from tkinter import filedialog
                        from merger_gui_10x import run_merge_10x_process

                        input_file = self.shared_data["input_file"]
                        folder = os.path.dirname( input_file )
                        all_csvs = [f for f in os.listdir( folder ) if f.lower().endswith( ".csv" )]
                        full_paths = [os.path.join( folder, f ) for f in sorted( all_csvs )]

                        second_file = None
                        if len( full_paths ) == 2:
                            # Avoid selecting the same file twice (normalize paths)
                            norm_input = os.path.normpath( input_file ).lower()
                            norm0 = os.path.normpath( full_paths[0] ).lower()
                            norm1 = os.path.normpath( full_paths[1] ).lower()

                            if norm_input == norm0:
                                second_file = full_paths[1]
                            elif norm_input == norm1:
                                second_file = full_paths[0]
                            else:
                                second_file = None  # No match, require manual selection

                            if second_file and os.path.normpath( second_file ).lower() == norm_input:
                                second_file = None  # Prevent using same file twice

                            if second_file:
                                print( f"[INFO] Auto-selected second file: {second_file}" )
                            else:
                                print( "[WARN] Could not auto-select a second file (same file detected)." )

                        if not second_file:
                            second_file = filedialog.askopenfilename(
                                title="Select Second File to Concatenate",
                                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
                            )

                        if not second_file:
                            messagebox.showerror( "File Missing", "Second file for concatenation was not selected." )
                            return

                        self.shared_data["second_file"] = second_file

                        run_merge_10x_process( self.shared_data, mode="basic" )

                    else:
                        print( f"[WARN] Function '{function_name}' not yet implemented in GUI stream mode." )
            except Exception as e:
                current_func = self.shared_data["function_queue"][0] if self.shared_data[
                    "function_queue"] else "unknown"
                print( f"[ERROR] Stream function '{current_func}' failed: {e}" )
                traceback.print_exc()
                messagebox.showerror( "Stream Error", f"An error occurred during '{current_func}':\n{e}" )
                self.shared_data["errors"].append( f"stream {current_func} failed" )
                return


        elif mode == "dask":
            print( "[EXECUTION] Routing to Dask-based full file processing..." )

            try:
                df = dd.read_csv(
                    self.shared_data["input_file"],
                    assume_missing=True,
                    dtype=str,
                    sep=self.shared_data["delimiter"],
                    quotechar=self.shared_data.get( "quotechar", '"' ) if self.shared_data["flags"].get(
                        "strip_quotes" ) else '"',
                    blocksize="64MB"
                )

            except Exception as e:
                print( f"[ERROR] Failed to load input file using Dask: {e}" )
                messagebox.showerror( "File Load Error", f"Dask failed to read the file:\n{e}" )
                self.shared_data["errors"].append( "Dask read_csv failed" )
                return

            self.shared_data["header"] = list( df.columns )

            # Handle add_stub_column
            if "add_stub_column" in self.shared_data["function_queue"]:
                print( "[INFO] Performing: add_stub_column" )
                config = get_stub_column_config(self.shared_data["header"], mode="fixed")
                if not config:
                    messagebox.showinfo( "Stub Skipped", "Stub column config was not provided. Skipping stub step." )
                else:
                    print( f"[INFO] Stub Config: {config}" )
                    df = df.map_partitions(
                        lambda partition: partition.apply(
                            lambda row: add_stub_column( row, config ), axis=1
                        )
                    )

            # Handle dedupe_records
            if "dedupe_records" in self.shared_data["function_queue"]:
                print( "[INFO] Performing: dedupe_records" )
                from deduper_8x import dedupe_records

                dedupe_fields = gui_select_fields(
                    "Select Field(s) to Dedupe By", self.shared_data["header"]
                )

                if not dedupe_fields:
                    messagebox.showinfo( "No Fields", "No dedupe fields selected. Skipping dedupe step." )
                else:
                    print( f"[INFO] Dedupe Fields: {dedupe_fields}" )

                    # Ask user which dedupe mode
                    keep_first = messagebox.askyesno(
                        "Dedupe Mode",
                        "Remove duplicate records?\n\n"
                        "Click YES to keep the first occurrence of each duplicate.\n"
                        "Click NO to keep only the duplicate records (discard uniques)."
                    )
                    mode = "dedupe" if keep_first else "keep_duplicates_only"
                    print( f"[INFO] Dedupe Mode: {mode}" )

                    try:
                        input_count = len( df )
                        df = dedupe_records( df, dedupe_fields=dedupe_fields, mode=mode )
                        output_count = len( df )

                        if mode == "dedupe":
                            rows_removed = input_count - output_count
                            summary = (
                                f"[INFO] Total rows before dedupe : {input_count:,}\n"
                                f"[INFO] Total rows after dedupe  : {output_count:,}\n"
                                f"[INFO] Duplicate rows removed    : {rows_removed:,}"
                            )
                        else:  # keep_duplicates_only
                            rows_kept = output_count
                            rows_dropped = input_count - output_count
                            summary = (
                                f"[INFO] Total rows before filtering : {input_count:,}\n"
                                f"[INFO] Duplicate rows kept         : {rows_kept:,}\n"
                                f"[INFO] Unique rows discarded       : {rows_dropped:,}"
                            )

                        print( "\n=== DEDUPE SUMMARY ===" )
                        print( summary )

                        messagebox.showinfo( "Dedupe Complete", f"{summary.replace( '[INFO]', '' ).strip()}" )

                    except Exception as e:
                        messagebox.showerror( "Dedupe Error", f"Failed during deduplication:\n{e}" )
                        self.shared_data["errors"].append( "dedupe_records failed" )
                        return


            # Handle sort_records
            if "sort_records" in self.shared_data["function_queue"]:
                from sorter_7x import sort_records

                sort_fields = gui_select_fields(
                    "Select Sort Fields", self.shared_data["header"]
                )

                if not sort_fields:
                    messagebox.showinfo( "No Sort Fields", "No sort fields selected. Skipping sort step." )
                else:
                    print( f"[INFO] Sorting by: {sort_fields}" )
                    sort_records(
                        df=df,
                        output_file=self.shared_data["output_file"],
                        sort_fields=sort_fields,
                        delimiter=self.shared_data["delimiter"]
                    )
                    messagebox.showinfo(
                        "Sort Complete",
                        f"Sorted output written to:\n{self.shared_data['output_file']}"
                    )
                    self.root.quit()
                    self.root.destroy()
                    sys.exit( 0 )

            # Handle split_file_by_condition
            if "split_file_by_condition" in self.shared_data["function_queue"]:
                try:
                    splitter_path = os.path.join( os.path.dirname( __file__ ), "splitter_9x_gui.py" )
                    spec_splitter = importlib.util.spec_from_file_location( "splitter", splitter_path )
                    splitter = importlib.util.module_from_spec( spec_splitter )
                    spec_splitter.loader.exec_module( splitter )

                    splitter.split_file_by_condition( self.shared_data )

                    messagebox.showinfo(
                        "Split Complete",
                        f"File was split based on condition.\nOutput saved in same folder as input."
                    )
                    self.root.quit()
                    self.root.destroy()
                    sys.exit( 0 )

                except Exception as e:
                    print( f"[ERROR] Split by condition failed: {e}" )
                    traceback.print_exc()
                    messagebox.showerror( "Split Error", f"Failed during split operation:\n{e}" )
                    self.shared_data["errors"].append( "split_file_by_condition failed." )
                    return

            # Handle split_by_composite_condition
            if "split_by_composite_condition" in self.shared_data["function_queue"]:
                try:
                    composite_path = os.path.join( os.path.dirname( __file__ ), "splitter_9x_composite_gui.py" )
                    spec_composite = importlib.util.spec_from_file_location( "splitter_composite", composite_path )
                    splitter_composite = importlib.util.module_from_spec( spec_composite )
                    spec_composite.loader.exec_module( splitter_composite )

                    splitter_composite.split_by_composite_condition( self.shared_data )

                    messagebox.showinfo(
                        "Composite Split Complete",
                        "File was split based on multiple conditions.\nOutput saved in same folder as input."
                    )
                    self.root.quit()
                    self.root.destroy()
                    sys.exit( 0 )

                except Exception as e:
                    print( f"[ERROR] Composite split failed: {e}" )
                    traceback.print_exc()
                    messagebox.showerror( "Split Error", f"Composite split error:\n{e}" )
                    self.shared_data["errors"].append( "split_by_composite_condition failed." )
                    return

        # Final output (if no sort triggered early exit)
            df.to_csv(
                self.shared_data["output_file"],
                single_file=True,
                index=False,
                sep=self.shared_data["delimiter"]
            )

            messagebox.showinfo(
                "Process Complete",
                f"Final output written to:\n{self.shared_data['output_file']}"
            )
            self.root.quit()
            self.root.destroy()
            sys.exit( 0 )

        elif mode == "standalone":
            print( "[EXECUTION] Performing: merge_by_key" )

            try:
                from tkinter import simpledialog

                # Step 1: Prompt user to select the key file
                key_file = filedialog.askopenfilename(
                    title="Select Key File",
                    filetypes=[("CSV/TXT Files", "*.csv *.txt"), ("All Files", "*.*")]
                )
                if not key_file:
                    if not self.shared_data.get( "merge_successful" ):
                        messagebox.showwarning( "Missing File", "No key file was selected. Merge cannot continue." )
                    return

                delimiter = self.shared_data["delimiter"]
                key_df = pd.read_csv( key_file, delimiter=delimiter, dtype=str ).fillna( "" )

                input_df = pd.read_csv( self.shared_data["input_file"], delimiter=delimiter, dtype=str ).fillna( "" )

                # Step 2: Auto-detect matching columns
                input_cols = set( input_df.columns.str.strip() )
                key_cols = set( key_df.columns.str.strip() )
                matched_cols = list( input_cols.intersection( key_cols ) )

                if matched_cols:
                    use_auto = messagebox.askyesno(
                        "Auto-Match Keys",
                        f"Matching columns found:\n{matched_cols}\n\nUse these as merge keys?"
                    )
                else:
                    use_auto = False

                if use_auto:
                    key_fields = matched_cols
                else:
                    key_fields = prompt_for_field_list( "merge key (from input file)", self.shared_data )
                    if not key_fields:
                        messagebox.showwarning( "No Keys", "No merge key fields selected." )
                        return

                print( f"[INFO] Using key fields: {key_fields}" )

                # Step 3: Join type prompt (include vs exclude)
                anti_join = not messagebox.askyesno(
                    "Include or Exclude Matches?",
                    "Would you like to keep records that MATCH the key file?\n\n"
                    "Click YES to keep matches (default inner join).\n"
                    "Click NO to exclude matches (anti-join)."
                )

                # Step 4: Normalize key fields (trim, lowercase, optional padding)
                def normalize_df(df, fields):
                    df = df.copy()
                    for field in fields:
                        df[field] = (
                            df[field]
                            .astype( str )
                            .str.strip()
                            .str.replace( '"', '', regex=False )
                            .str.lower()
                        )
                    return df

                input_df_norm = normalize_df( input_df, key_fields )
                key_df_norm = normalize_df( key_df, key_fields )

                key_tuples = set( tuple( row ) for _, row in key_df_norm[key_fields].iterrows() )
                match_flags = input_df_norm[key_fields].apply( lambda row: tuple( row ) in key_tuples, axis=1 )

                if anti_join:
                    result_df = input_df[~match_flags].copy()
                    secondary_df = input_df[match_flags].copy()
                    secondary_label = "_MATCH"
                else:
                    result_df = input_df[match_flags].copy()
                    secondary_df = input_df[~match_flags].copy()
                    secondary_label = "_NOMATCH"

                preview = f"{len( result_df ):,} record(s) will be {'EXCLUDED' if anti_join else 'included'}.\nProceed?"
                if not messagebox.askyesno( "Confirm Output", preview ):
                    print( "[INFO] User cancelled after preview." )
                    return

                # Step 5: Write result file
                result_df.to_csv(
                    self.shared_data["output_file"],
                    index=False,
                    sep=delimiter
                )

                # Step 6: Write secondary (matched/unmatched) file
                base, ext = os.path.splitext( self.shared_data["output_file"] )
                secondary_path = f"{base}{secondary_label}{ext}"
                secondary_df.to_csv(
                    secondary_path,
                    index=False,
                    sep=delimiter
                )

                print( f"[INFO] Main output → {self.shared_data['output_file']}" )
                print( f"[INFO] Secondary   → {secondary_path}" )

                messagebox.showinfo(
                    "Merge-by-Key Complete",
                    f"{len( result_df ):,} records {'excluded' if anti_join else 'retained'}.\n"
                    f"Output written to:\n{os.path.basename( self.shared_data['output_file'] )}\n\n"
                    f"Secondary file → {os.path.basename( secondary_path )}"
                )

                self.root.quit()
                self.root.destroy()
                sys.exit( 0 )

            except Exception as e:
                current_func = self.shared_data["function_queue"][0] if self.shared_data[
                    "function_queue"] else "unknown"
                print( f"[ERROR] Standalone function '{current_func}' failed: {e}" )
                traceback.print_exc()
                messagebox.showerror( "Standalone Error", f"An error occurred during '{current_func}':\n{e}" )
                self.shared_data["errors"].append( f"standalone {current_func} failed" )
                return

        else:
            print( "[ERROR] Unrecognized mode after routing logic." )
            self.shared_data["errors"].append( "Unrecognized execution mode." )


if __name__ == "__main__":
    root = tk.Tk()
    app = FileUtilityApp(root)
    root.mainloop()
    import sys

    sys.exit( 0 )


