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
║   └─ user_interface.capture_user_inputs_gui()                        ║
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

# === LOGGING SETUP (overwrite mode) ===
import logging

LOG_FILE = "file_util_log.txt"

logging.basicConfig(
    filename=LOG_FILE,             # Writes log to script's working directory
    filemode="w",                  # ✅ Overwrite log file each run
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger()

import os
import sys
import subprocess
import csv
import importlib.util
import warnings
import tkinter as tk
from tkinter import Toplevel, Listbox, Label, Button, EXTENDED, messagebox, filedialog, ttk
from match_mapper_gui import get_field_mapping
from dask.config import config
from utils_11x_gui import safe_open
from quote_detector import detect_field_quote_pattern
from stub_loader import load_stub_lookup_table_v2
from filters_ui_3x_gui import collect_filter_rules_gui
from scissors_ui_6x_gui import ScissorsDialog
from key_merge_ui_12x_gui import KeyMergeDialog
# --- Option 9 (Split-by-Key) UI ---
try:
    from key_split_ui_9x_gui import run_key_split_workflow_gui
except Exception:
    run_key_split_workflow_gui = None  # handled gracefully at runtime
# --- Option 11 (Concatenate) UI ---
try:
    from concatenator_ui_11x_gui import ConcatenateFilesDialog
except Exception:
    ConcatenateFilesDialog = None  # handled gracefully at runtime
# --- Dedupe UI + engine ---
from dedupe_ui_8x import prompt_dedupe_mode, prompt_dedupe_fields



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
loader_path = os.path.join(os.path.dirname(__file__), "2x_File_Loader_gui.py")
spec = importlib.util.spec_from_file_location("file_loader", loader_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Unable to load file_loader from {loader_path}")

file_loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(file_loader)

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
                "quote_preserve": True,
                "flexible_decoding": False
            },
            "has_header": False,
            "header": [],
            "filter_rules": [],
            "results": {},
            "errors": [],
            "chunk_size": 1
        }

    def prompt_for_key_fields(self):
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

        if not input_headers:
            input_file = self.shared_data.get("input_file")
            delimiter = self.shared_data.get("delimiter", ",")
            try:
                f, enc_used, fallback = safe_open(
                    input_file,
                    mode="r",
                    newline="",
                    flexible=config["flags"].get("flexible_decoding", False)
                )

                if fallback:
                    print(f"[INFO] Opened input file using fallback encoding: {enc_used}")
                else:
                    print(f"[INFO] Opened input file using preferred encoding: {enc_used}")

                with f:
                    reader = csv.reader(f, delimiter=delimiter)
                    first_row = next(reader, [])
                    input_headers = [f"Field {i + 1}" for i in range(len(first_row))]

                # 🔍 If quote_map wasn't set earlier, generate a basic one now
                if "quote_map" not in self.shared_data:
                    from quote_detector import detect_field_quote_pattern
                    quote_map = detect_field_quote_pattern(input_file, delimiter=delimiter, encoding=enc_used)
                    self.shared_data["quote_map"] = quote_map
                    print(f"[DEBUG] [Key Field Prompt] Detected quote map: {quote_map}")

            except Exception as e:
                messagebox.showerror("Header Load Error", f"Could not determine header fields:\n{e}")
                popup.destroy()
                return

        # Populate the listbox
        # Populate the listbox with indexed labels
        for i, field in enumerate( input_headers ):
            label = f"{i} – {field}"
            field_list.insert( "end", label )

        def prompt_for_keyfile_fields(key_file_path, field_count):
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
                with open( key_file_path, "r", encoding="utf-8", newline="" ) as f:
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

        # misc. variables here
        self.output_path_var = tk.StringVar()

        # Row 0 — Step 1 label
        ttk.Label(frame, text="Step 1: Load Input File").grid(row=0, column=0, sticky="w")

        # Row 1 — Select file button
        self.load_btn = ttk.Button(frame, text="Select File", command=self.load_input_file)
        self.load_btn.grid(row=1, column=0, pady=5, sticky="w")

        # Row 2 — File label
        self.file_label = ttk.Label(frame, text="No file selected.")
        self.file_label.grid(row=2, column=0, sticky="w")

        # Row 3 — Header checkbox
        self.has_header_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Input file contains a header record",
            variable=self.has_header_var
        ).grid(row=3, column=0, pady=(10, 0), sticky="w")

        # Row 4 — Case sensitivity checkbox
        self.case_sensitive_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Enable case-sensitive filtering",
            variable=self.case_sensitive_var
        ).grid(row=4, column=0, sticky="w")

        # Row 5 — Quote stripping checkbox
        self.strip_quotes_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Strip leading/trailing quotes from fields",
            variable=self.strip_quotes_var
        ).grid(row=5, column=0, sticky="w")

        # Row 6 — Flexible decoding checkbox
        self.flexible_decoding_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frame,
            text="Enable flexible encoding fallback (try CP1252, Latin-1 if UTF-8 fails)",
            variable=self.flexible_decoding_var
        ).grid(row=6, column=0, sticky="w")

        # Row 7 — Normalization checkbox
        self.normalize_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frame,
            text="Normalize values before processing",
            variable=self.normalize_var
        ).grid(row=7, column=0, sticky="w")

        # Row 9 — Function selection label
        ttk.Label(frame, text="Step 2: Select Functions to Apply").grid(row=9, column=0, pady=(15, 0), sticky="w")

        # Row 10 — Function listbox
        self.function_listbox = tk.Listbox(frame, selectmode=tk.MULTIPLE, height=10, exportselection=False)
        self.function_listbox.grid(row=10, column=0, sticky="w", pady=5)

        # Define GUI-only function options
        self.function_options = {
            "1": "filter_omit",
            "2": "filter_select",
            "3": "replace_rec_contents",
            "4": "add_rec_stub_(fixed)",
            "5": "add_rec_stub_(var_from_rec_contents)",
            "6": "Scissors — Column Chooser",
            "7": "sort_records",
            "8": "dedupe_records",
            "9": "split_by_key",
            "10": "split_by_composite_condition",
            "11": "concatenate_files",
            "12": "merge_by_key",
            "13": "file_compare"
        }

        for key in sorted(self.function_options.keys(), key=lambda x: int(x)):
            label = f"{key}. {self.function_options[key]}"
            self.function_listbox.insert(tk.END, label)

        # Row 11 — Continue button
        self.continue_btn = ttk.Button(
            frame,
            text="Continue",
            command=self.on_continue
        )
        self.continue_btn.grid(row=11, column=0, pady=(15, 0), sticky="w")

    def load_input_file(self):
        filepath = filedialog.askopenfilename(
            title="Select Input File",
            filetypes=[("CSV/TXT Files", "*.csv *.txt"), ("All Files", "*.*")]
        )
        if filepath:
            self.shared_data["input_file"] = filepath
            self.file_label.config(text=os.path.basename(filepath))
            messagebox.showinfo("File Selected", f"Loaded file:\n{filepath}")

            # ✅ Automatically generate matching output filename
            if filepath.endswith(".csv"):
                output_path = filepath.replace(".csv", "._RESULTS.csv")
            else:
                output_path = filepath + "._RESULTS.csv"

            self.output_path_var.set(output_path)
            self.shared_data["output_file"] = output_path  # Optional: store immediately here too

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
        """
        Main continue handler.
        - Uses the preloaded file_loader (top-level) to detect delimiter
        - Reads header for the main input
        - (If stub var-from-records is selected) prompts for stub file and launches field mapping GUI
        - Captures flags and routes execution (including optional dedupe picker)
        """

        print("[OC] enter on_continue")
        # --- FAST-PATH: If ONLY "concatenate_files" is selected, launch the dialog now ---
        try:
            selected_indices_fp = self.function_listbox.curselection()
            selected_labels_fp = [self.function_listbox.get(i) for i in selected_indices_fp]
            selected_functions_fp = [lbl.split(". ", 1)[1] for lbl in selected_labels_fp]
        except Exception:
            selected_functions_fp = []

        if selected_functions_fp and all(fn == "concatenate_files" for fn in selected_functions_fp):
            if ConcatenateFilesDialog is None:
                messagebox.showerror("Option 11", "Concatenator UI not available (import failed).")
                return
            # Seed last_input_dir from the selected input file (if any)
            if not self.shared_data.get("last_input_dir") and self.shared_data.get("input_file"):
                self.shared_data["last_input_dir"] = os.path.dirname(self.shared_data["input_file"])

            dlg = ConcatenateFilesDialog(self.root, self.shared_data)
            _ = dlg.open()  # returns after the dialog closes

            # Show the standard completion popup and exit cleanly (fast path = only Option 11)
            try:
                messagebox.showinfo("File Utility Complete", "All selected function(s) completed successfully.",
                                    parent=self.root)
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists():
                    self.root.quit()
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists():
                    self.root.destroy()
            except Exception:
                pass
            return

        # --- FAST-PATH: If ONLY "Scissors — Column Chooser" is selected, launch it now ---
        try:
            selected_indices_sc = self.function_listbox.curselection()
            selected_labels_sc = [self.function_listbox.get(i) for i in selected_indices_sc]
            selected_functions_sc = [lbl.split(". ", 1)[1] for lbl in selected_labels_sc]
        except Exception:
            selected_functions_sc = []

        if selected_functions_sc and all(fn.lower().startswith("scissors") for fn in selected_functions_sc):
            try:
                dlg = ScissorsDialog(self.root, self.shared_data)
                _ = dlg.open()  # modal; handles preview+run, writes report
            except Exception as e:
                messagebox.showerror("Option 6", f"Could not launch Scissors UI:\n{e}")
                return

            # Standard completion popup and clean close (match Option 11 style)
            try:
                messagebox.showinfo("File Utility Complete", "All selected function(s) completed successfully.",
                                    parent=self.root)
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists():
                    self.root.quit()
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists():
                    self.root.destroy()
            except Exception:
                pass
            return

        # Ensure previous run doesn't suppress popups or reuse old replace config
        self.shared_data.pop("end_popup_shown", None)
        if "function_queue" in self.shared_data and "replace_rec_contents" in self.shared_data["function_queue"]:
            self.shared_data.pop("replace_config", None)  # force UI prompt for replace each run

        # Release any stray grabs from prior dialogs (harmless if none)
        try:
            if self.root and self.root.winfo_exists():
                self.root.grab_release()
        except Exception:
            pass

        # --- FAST-PATH: If ONLY "merge_by_key" is selected, launch it now ---
        try:
            sel_idx_mk = self.function_listbox.curselection()
            sel_labels_mk = [self.function_listbox.get(i) for i in sel_idx_mk]
            sel_funcs_mk = [lbl.split(". ", 1)[1] for lbl in sel_labels_mk]
        except Exception:
            sel_funcs_mk = []

        if sel_funcs_mk and all(fn == "merge_by_key" for fn in sel_funcs_mk):
            try:
                dlg = KeyMergeDialog(self.root, self.shared_data)
                _ = dlg.open()
            except Exception as e:
                messagebox.showerror("Option 12", f"Could not launch Merge-by-Key UI:\n{e}")
                return
            try:
                messagebox.showinfo("File Utility Complete",
                                    "All selected function(s) completed successfully.", parent=self.root)
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists(): self.root.quit()
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists(): self.root.destroy()
            except Exception:
                pass
            return

        # --- FAST-PATH: If ONLY "split_by_key" is selected, launch it now ---
        try:
            sel_idx_k9 = self.function_listbox.curselection()
            sel_labels_k9 = [self.function_listbox.get(i) for i in sel_idx_k9]
            sel_funcs_k9 = [lbl.split(". ", 1)[1] for lbl in sel_labels_k9]
        except Exception:
            sel_funcs_k9 = []

        if sel_funcs_k9 and all(fn == "split_by_key" for fn in sel_funcs_k9):
            if run_key_split_workflow_gui is None:
                messagebox.showerror("Option 9", "Key Split UI not available (import failed).")
                return
            try:
                _ = run_key_split_workflow_gui(self.shared_data, logger)
            except Exception as e:
                messagebox.showerror("Option 9", f"Could not launch Split-by-Key UI:\n{e}")
                return
            try:
                messagebox.showinfo("File Utility Complete",
                                    "All selected function(s) completed successfully.", parent=self.root)
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists(): self.root.quit()
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists(): self.root.destroy()
            except Exception:
                pass
            return

        # --- FAST-PATH: If ONLY "file_compare" is selected, launch the stand-alone GUI now ---
        try:
            selected_indices_fc = self.function_listbox.curselection()
            selected_labels_fc = [self.function_listbox.get(i) for i in selected_indices_fc]
            selected_functions_fc = [lbl.split(". ", 1)[1] for lbl in selected_labels_fc]
        except Exception:
            selected_functions_fc = []

        if selected_functions_fc and all(fn == "file_compare" for fn in selected_functions_fc):
            try:
                script = os.path.join(os.path.dirname(__file__), "Two_File_Compare_gui.py")
                # Launch as separate process to avoid multiple Tk roots in one process
                subprocess.Popen([sys.executable, script])
            except Exception as e:
                messagebox.showerror("Option 13", f"Could not launch File Compare UI:\n{e}")
            return  # nothing else to do in 0x for this path

        try:
            # --- 1) Detect delimiter for main input (use preloaded file_loader) ---
            # Allow continue if concatenate or file_compare is selected (both are stand-alone)
            pending_labels = [self.function_listbox.get(i) for i in self.function_listbox.curselection()]
            pending_functions = [lbl.split(". ", 1)[1] for lbl in pending_labels]
            if not self.shared_data.get("input_file") and not (
                    {"concatenate_files", "file_compare"} & set(pending_functions)):
                messagebox.showerror("Missing Input", "Please select an input file before continuing.")
                return

            delimiter = file_loader.detect_delimiter(
                self.shared_data["input_file"],
                shared_data=self.shared_data
            )
            self.shared_data["delimiter"] = delimiter
            # >>> ADD: publish the output delimiter for downstream writers
            self.shared_data["output_delimiter"] = delimiter

            # --- 2) Read main header ---
            with open(self.shared_data["input_file"], mode="r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader, [])
            if not header:
                messagebox.showerror("Header Load Error", "Unable to read header from the input file.")
                return
            self.shared_data["header"] = header
            self.shared_data["input_header"] = header

            # Ensure has_header is available for mapping payloads that follow
            self.shared_data["has_header"] = self.has_header_var.get()

            # Pre-capture selection so we know which modals to launch (stub, dedupe, etc.)
            selected_indices = self.function_listbox.curselection()
            selected_functions = [self.function_listbox.get(i).split('. ', 1)[1] for i in selected_indices]
            self.shared_data["functions_selected"] = selected_functions
            self.shared_data["function_queue"] = self.shared_data.get("functions_selected", [])

            # --- Collect filter rules if any filter function was chosen ---
            if any(fn in ("filter_select", "filter_omit") for fn in self.shared_data["function_queue"]):
                rules = collect_filter_rules_gui(self.shared_data["header"])
                if not rules:
                    messagebox.showwarning("No Filters", "No filter rules were entered. Filtering was canceled.")
                    return
                self.shared_data["filter_rules"] = rules
                logger.info(f"[FILTER] Collected {len(rules)} rule(s) from GUI.")
                # Ensure filter flags exist (affects how apply_filters compares)
                flags = self.shared_data.setdefault("flags", {})
                flags.setdefault("strip_quotes", True)  # treat "Kansas" and Kansas the same
                flags.setdefault("case_sensitive", False)  # make text matches case-insensitive by default

            # --- 3) If variable stub is selected, prompt for stub file + mapping ---
            if "add_rec_stub_(var_from_rec_contents)" in self.shared_data.get("function_queue", []):
                stub_file = filedialog.askopenfilename(
                    title="Select STUB file for mapping",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
                )
                if not stub_file:
                    messagebox.showwarning("Stub Cancelled", "No stub file selected.")
                    return

                # sniff stub delimiter with the same loader
                stub_delim = file_loader.detect_delimiter(stub_file, shared_data=self.shared_data)
                with open(stub_file, mode="r", encoding="utf-8", newline="") as f:
                    reader = csv.reader(f, delimiter=stub_delim)
                    stub_header = next(reader, [])

                # launch mapping GUI (top-level import already available)
                mapping_result = get_field_mapping(
                    input_header=self.shared_data["header"],
                    stub_header=stub_header,
                    logger=self.shared_data.get("logger")
                )
                if not mapping_result or not mapping_result.get("match_fields") or not mapping_result.get(
                        "stub_fields"):
                    messagebox.showerror("Mapping Error", "Field mapping was not completed.")
                    return

                # persist mapping + stub file
                self.shared_data["stub_field_mapping"] = mapping_result
                self.shared_data["external_stub_file"] = stub_file

                # Prepare final stub_config for downstream
                mapping_result["mode"] = "from_file"
                mapping_result["stub_file"] = stub_file
                mapping_result["main_delimiter"] = self.shared_data.get("delimiter")
                mapping_result["main_has_header"] = self.shared_data.get("has_header", True)
                mapping_result["input_header"] = self.shared_data["header"]
                mapping_result["input_file"] = self.shared_data["input_file"]

                self.shared_data["stub_config"] = mapping_result

                # --- Compose composite header for var-from-records stub ---
                def _to_names(fields, header_row):
                    out = []
                    for f in fields:
                        if isinstance(f, int):
                            if 0 <= f < len(header_row):
                                out.append(str(header_row[f]))
                            else:
                                out.append(str(f))  # safe fallback
                        else:
                            out.append(str(f))
                    return out

                # IMPORTANT: reuse the stub_header already read earlier (do NOT re-read)
                # stub_header must be the same one passed into get_field_mapping(...)
                # Fallbacks if something unexpected happens:
                hdr_for_names = stub_header if stub_header else mapping_result.get("stub_header") or []

                # Map selected stub fields -> names (order must match appended data columns)
                chosen_stub_names = _to_names(mapping_result.get("stub_fields", []), hdr_for_names)

                # De-duplicate stub names to avoid identical headers
                base_header = list(self.shared_data["header"])
                existing = {name: 1 for name in base_header}
                dedup_stub_names = []
                counts = {}
                for name in chosen_stub_names:
                    if name in existing or counts.get(name, 0) > 0:
                        counts[name] = counts.get(name, 0) + 1
                        dedup_stub_names.append(f"{name} (stub{'' if counts[name] == 1 else counts[name]})")
                    else:
                        counts[name] = 0
                        dedup_stub_names.append(name)

                composite_header = base_header + dedup_stub_names
                self.shared_data["output_header"] = composite_header
                self.shared_data["header_written"] = False
                self.shared_data["write_header"] = True
                self.shared_data["output_delimiter"] = self.shared_data.get("output_delimiter") or self.shared_data.get(
                    "delimiter") or ";"
                logger.info(f"[HEADER] Composite header prepared ({len(composite_header)} cols)")



        except Exception as e:
            messagebox.showerror("Input / Mapping Error", f"{e}")
            return

        # --- 4) Capture flags and finalize queue (no function-level imports of os) ---
        try:
            self.capture_function_selection()
            self.shared_data["has_header"] = self.has_header_var.get()
            self.shared_data["flags"]["case_sensitive"] = self.case_sensitive_var.get()
            self.shared_data["flags"]["strip_quotes"] = self.strip_quotes_var.get()
            self.shared_data["flags"]["flexible_decoding"] = self.flexible_decoding_var.get()
            self.shared_data["normalize"] = self.normalize_var.get()

            # quote preservation derived from strip_quotes
            self.shared_data["quote_preserve"] = not self.shared_data["flags"].get("strip_quotes", False)

            # output path info
            self.shared_data["output_file"] = self.output_path_var.get()
            self.shared_data["output_folder"] = os.path.dirname(self.output_path_var.get())
            self.shared_data["logger"] = logger

            if not self.shared_data["function_queue"]:
                messagebox.showerror("No Function Selected",
                                     "Please select at least one function before continuing.")
                return

            # detect field-level quoting (best-effort, don’t abort on failure)
            try:
                quote_map = detect_field_quote_pattern(
                    self.shared_data["input_file"],
                    delimiter=self.shared_data["delimiter"],
                    encoding="utf-8"
                )
                self.shared_data["quote_map"] = quote_map
                print(f"[DEBUG] Field-level quote structure: {quote_map}")
                self.shared_data["field_quote_flags"] = quote_map

            except Exception as qerr:
                print(f"[WARN] Quote detection failed: {qerr}")

            # ✅ Prompt user for fixed stub value if fixed mode selected
            if "add_rec_stub_(fixed)" in self.shared_data["function_queue"]:
                try:
                    import importlib.util
                    config_path = os.path.join(os.path.dirname(__file__), "stub_5x_ui_config_gui.py")
                    spec = importlib.util.spec_from_file_location("stub_config", config_path)
                    stub_config = importlib.util.module_from_spec(spec)

                    if self.shared_data.get("logger"):
                        self.shared_data["logger"].debug(
                            f"[FINAL MAPPING] stub_fields = {self.shared_data.get('stub_config', {}).get('stub_fields')}")
                        self.shared_data["logger"].debug(
                            f"[FINAL MAPPING] full stub_config = {self.shared_data.get('stub_config')}")

                    spec.loader.exec_module(stub_config)

                    result = stub_config.get_stub_column_config(self.shared_data.get("header", []))

                    if result and result.get("value") is not None:
                        self.shared_data["fixed_stub_fields"] = [result["value"]]
                        self.shared_data["fixed_stub_field_name"] = result["new_column"]
                    else:
                        messagebox.showwarning("Stub Cancelled", "No fixed stub value provided.")
                        return

                    # --- Header policy for fixed stub ---
                    try:
                        fixed_col = self.shared_data.get("fixed_stub_field_name")
                        if fixed_col:
                            base_header = list(self.shared_data["header"])
                            # De-duplicate single fixed stub name if it already exists
                            if fixed_col in base_header:
                                fixed_col_out = f"{fixed_col} (stub)"
                            else:
                                fixed_col_out = fixed_col

                            composite_header = base_header + [fixed_col_out]
                            self.shared_data["output_header"] = composite_header
                            self.shared_data["header_written"] = False
                            self.shared_data["write_header"] = True

                    except Exception as _hdr_err:
                        # Non-fatal: if anything odd happens, we’ll just let writers fall back to input header
                        logger.warning(f"[HEADER] Fixed-stub header composition warning: {_hdr_err}")


                except Exception as e:
                    messagebox.showerror("Fixed Stub Error", f"Failed to configure fixed stub:\n{e}")
                    return

        except Exception as e:
            messagebox.showerror("Flag Capture Error", f"{e}")
            return

        # --- 5) Optional: Dedupe field picker wiring (only if dedupe selected) ---
        # === DEDUPE FRONTEND (single prompt + cache) ===
        if "dedupe_records" in self.shared_data.get("function_queue", []):
            # Only prompt once; cache for backend
            if not self.shared_data.get("dedupe_mode"):
                mode = prompt_dedupe_mode()  # UI prompt ONCE
                if mode is None:
                    messagebox.showinfo(title="Canceled", message="Dedupe canceled by user.")
                    return
                self.shared_data["dedupe_mode"] = mode

            # Always use the CURRENT file’s header for the field picker
            header_for_picker = self.shared_data.get("header") or self.shared_data.get("output_header") or []
            fields = prompt_dedupe_fields(header_for_picker)
            if not fields:
                messagebox.showinfo(title="Canceled", message="No columns selected for dedupe.")
                return

            # Cache for backend
            self.shared_data["dedupe_fields"] = fields

        # --- 6) Route execution ---
        # If file_compare was selected with other functions, launch it here first.
        if "file_compare" in self.shared_data.get("function_queue", []):
            try:
                script = os.path.join(os.path.dirname(__file__), "Two_File_Compare_gui.py")
                subprocess.Popen([sys.executable, script])
            except Exception as e:
                messagebox.showerror("Option 13", f"Could not launch File Compare UI:\n{e}")
                # Do not return; we still remove it from queue and proceed with others
            # Remove it from the queue so the stream/router path doesn't see it
            self.shared_data["function_queue"] = [
                fn for fn in self.shared_data["function_queue"] if fn != "file_compare"
            ]

        # If concatenation was selected along with other functions, run it here first.
        if "concatenate_files" in self.shared_data.get("function_queue", []):
            if ConcatenateFilesDialog is None:
                messagebox.showerror("Option 11", "Concatenator UI not available (import failed).")
                return
            if not self.shared_data.get("last_input_dir") and self.shared_data.get("input_file"):
                self.shared_data["last_input_dir"] = os.path.dirname(self.shared_data["input_file"])
            dlg = ConcatenateFilesDialog(self.root, self.shared_data)
            _ = dlg.open()  # dialog handles preview + run, updates shared_data
            # Remove it from the queue so function_router doesn't see it
            self.shared_data["function_queue"] = [
                fn for fn in self.shared_data["function_queue"] if fn != "concatenate_files"
            ]

        # If Scissors was selected along with other functions, run it here first.
        if any(fn.lower().startswith("scissors") for fn in self.shared_data.get("function_queue", [])):
            try:
                dlg = ScissorsDialog(self.root, self.shared_data)
                _ = dlg.open()  # dialog handles preview + run
            except Exception as e:
                messagebox.showerror("Option 6", f"Could not launch Scissors UI:\n{e}")
                return
            # Remove Scissors from the queue so the stream/router path doesn't see it
            self.shared_data["function_queue"] = [
                fn for fn in self.shared_data["function_queue"] if not fn.lower().startswith("scissors")
            ]

        # If merge_by_key was selected along with others, run it here first.
        if any(fn == "merge_by_key" for fn in self.shared_data.get("function_queue", [])):
            try:
                dlg = KeyMergeDialog(self.root, self.shared_data)
                _ = dlg.open()
            except Exception as e:
                messagebox.showerror("Option 12", f"Could not launch Merge-by-Key UI:\n{e}")
                return
            self.shared_data["function_queue"] = [
                fn for fn in self.shared_data["function_queue"] if fn != "merge_by_key"
            ]

        # If split_by_key was selected along with others, run it here first.
        if any(fn == "split_by_key" for fn in self.shared_data.get("function_queue", [])):
            if run_key_split_workflow_gui is None:
                messagebox.showerror("Option 9", "Key Split UI not available (import failed).")
                return
            try:
                _ = run_key_split_workflow_gui(self.shared_data, logger)
            except Exception as e:
                messagebox.showerror("Option 9", f"Could not launch Split-by-Key UI:\n{e}")
                return
            # Remove it so function_router won't see it
            self.shared_data["function_queue"] = [
                fn for fn in self.shared_data["function_queue"] if fn != "split_by_key"
            ]

        def _safe_message(kind: str, title: str, body: str):
            import tkinter as tk
            from tkinter import messagebox
            temp = None
            parent = None
            try:
                if getattr(self, "root", None) and hasattr(self.root, "winfo_exists") and self.root.winfo_exists():
                    parent = self.root
                else:
                    temp = tk.Tk()
                    temp.withdraw()
                    parent = temp

                if kind == "info":
                    messagebox.showinfo(title, body, parent=parent)
                elif kind == "error":
                    messagebox.showerror(title, body, parent=parent)
                else:
                    messagebox.showwarning(title, body, parent=parent)
            except Exception:
                try:
                    if temp is None:
                        temp = tk.Tk();
                        temp.withdraw()
                    if kind == "info":
                        messagebox.showinfo(title, body, parent=temp)
                    elif kind == "error":
                        messagebox.showerror(title, body, parent=temp)
                    else:
                        messagebox.showwarning(title, body, parent=temp)
                except Exception:
                    pass
            finally:
                try:
                    if temp is not None:
                        temp.destroy()
                except Exception:
                    pass

        try:
            # --- Unsupported functions gate (show info + skip) ---
            unsupported = {
                "split_by_composite_condition"
                # add more here as needed
            }
            pending = [fn for fn in self.shared_data.get("function_queue", []) if fn in unsupported]
            if pending:
                # 1) Inform the user that these functions are not available (single dialog)
                _safe_message(
                    "info",
                    "Function Not Available",
                    "* * Function(s) Not Available at this Time * *\n\n"
                    + "\n".join(f"• {fn}" for fn in pending)
                    + "\n\nThey will be skipped."
                )
                # 2) Remove them from the queue
                self.shared_data["function_queue"] = [
                    fn for fn in self.shared_data["function_queue"] if fn not in unsupported
                ]
                # 3) If nothing left to run, show the normal completion popup and exit cleanly
                if not self.shared_data["function_queue"]:
                    if not self.shared_data.get("end_popup_shown"):
                        self.shared_data["end_popup_shown"] = True
                        _safe_message("info", "File Utility Complete",
                                      "All selected function(s) completed successfully.")
                    try:
                        if self.root and self.root.winfo_exists():
                            self.root.quit()
                    except Exception:
                        pass
                    try:
                        if self.root and self.root.winfo_exists():
                            self.root.destroy()
                    except Exception:
                        pass
                    return

            from function_router import function_routing
            print("[OC] before function_routing")
            # Parent for any child dialogs
            self.shared_data["root"] = self.root
            # Make sure UI is responsive and any pending events are flushed
            try:
                self.root.update_idletasks()
            except Exception:
                pass

            function_routing(self.shared_data)

            print("[OC] after function_routing (success path)")
            if not self.shared_data.get("end_popup_shown"):
                self.shared_data["end_popup_shown"] = True
                # Try to bring main window to front; ignore if already gone
                try:
                    if self.root and self.root.winfo_exists():
                        self.root.deiconify();
                        self.root.lift();
                        self.root.focus_force()
                except Exception:
                    pass
                _safe_message("info", "File Utility Complete", "All selected function(s) completed successfully.")

            # Clean shutdown if the root still exists
            try:
                if self.root and self.root.winfo_exists():
                    self.root.quit()
            except Exception:
                pass
            try:
                if self.root and self.root.winfo_exists():
                    self.root.destroy()
            except Exception:
                pass

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print("[OC][EXC] exception in on_continue\n", tb)
            _safe_message("error", "File Utility Error", f"{e}\n\n{tb}")
            try:
                if self.root and self.root.winfo_exists():
                    self.root.quit();
                    self.root.destroy()
            except Exception:
                pass

        # schedule after returning to the Tk event loop
        # self.root.after(0, _finalize)

def apply_stub_logic(input_path, output_path, config, normalize_config=None, logger=None, shared_data=None):
    """
    Applies stub logic (fixed or from_file) to each record and writes to output.
    Logs summary with match/miss counts.
    """
    if logger is None:
        logger = logging.getLogger()

    # --- Dynamic load of stub logic ---
    stub_logic_path = os.path.join(os.path.dirname(__file__), "stub_5x_gui.py")
    spec = importlib.util.spec_from_file_location("stub_5x_gui", stub_logic_path)
    stub_5x_gui = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stub_5x_gui)

    dispatch_stub_logic = stub_5x_gui.dispatch_stub_logic
    load_stub_lookup_table = load_stub_lookup_table_v2

    prepare_stub_key_config = stub_5x_gui.prepare_stub_key_config
    from stub_utils import denormalize_keys  # Already exists in your system
    from utils_11x_gui import safe_open

    infile, in_enc, _ = safe_open(input_path, mode="r", newline="")
    outfile, out_enc, _ = safe_open(output_path, mode="w", newline="")
    logger.info(f"[INFO] Input encoding:  {in_enc}")
    logger.info(f"[INFO] Output encoding: {out_enc}")

    # Ensure header emission uses the same delimiter as rows
    shared_data = (shared_data or {})
    shared_data.setdefault("output_delimiter", config.get("main_delimiter", ";"))
    shared_data.setdefault("write_header", True)
    shared_data.setdefault("header_written", False)

    # If GUI didn’t publish a composite header, try to build one from known stub names (if present)
    if not shared_data.get("output_header"):
        stub_names_for_header = config.get("stub_names")  # from load_stub_lookup_table_v2
        if stub_names_for_header:
            # Prefer a header we already know
            base_header = config.get("input_header") or shared_data.get("input_header")

            # If still unknown and the main file has a header, read the first row from input_path
            if not base_header and config.get("main_has_header"):
                try:
                    f, enc_used, fallback = safe_open(
                        input_path,
                        mode="r",
                        newline="",
                        flexible=bool(shared_data.get("flags", {}).get("flexible_decoding"))
                    )
                    with f:
                        reader = csv.reader(f, delimiter=config["main_delimiter"])
                        base_header = next(reader, []) or []
                except Exception as e:
                    if logger:
                        logger.warning(f"[0x] Could not read header to build output_header: {e}")
                    base_header = []

            if base_header:
                # De-duplicate appended names: add " (stub)" suffix if they clash
                existing = {name: 1 for name in base_header}
                counts = {}
                dedup = []
                for nm in stub_names_for_header:
                    if nm in existing or counts.get(nm, 0) > 0:
                        counts[nm] = counts.get(nm, 0) + 1
                        suffix = "" if counts[nm] == 1 else counts[nm]
                        dedup.append(f"{nm} (stub{suffix})")
                    else:
                        counts[nm] = 0
                        dedup.append(nm)

                shared_data["output_header"] = list(base_header) + dedup

    # --- Local header/row helpers (mirrors stream_router) ---
    def _ensure_header_once(outfile, shared_data, logger):
        if (not shared_data.get("header_written")
                and shared_data.get("output_header")
                and shared_data.get("write_header", True)):
            delim = shared_data.get("output_delimiter", ";")
            header_line = delim.join(map(str, shared_data["output_header"])) + "\n"
            outfile.write(header_line)
            shared_data["header_written"] = True
            if logger:
                try:
                    logger.info(f"[HEADER] Wrote composite header ({len(shared_data['output_header'])} cols)")
                except Exception:
                    logger.info("[HEADER] Wrote composite header")

    def _write_record(outfile, record, delimiter, quote_map, logger, shared_data):
        # Emit header (once) before the first row
        _ensure_header_once(outfile, shared_data, logger)
        # Preserve field-level quoting pattern
        try:
            quoted = []
            for i, val in enumerate(record):
                s = "" if val is None else str(val)
                if i < len(quote_map) and quote_map[i]:
                    s = s.replace('"', '""')
                    quoted.append(f'"{s}"')
                else:
                    quoted.append(s)
            outfile.write(delimiter.join(quoted) + "\n")
        except Exception as e:
            if logger:
                logger.warning(f"[QUOTE FALLBACK] Failed to write record: {e}")
            # last resort: naive join
            outfile.write(delimiter.join("" if v is None else str(v) for v in record) + "\n")

    total_rows = 0
    stub_matches = 0
    stub_misses = 0

    # --- STEP 1: Load stub lookup (v2 centralized loader) ---

    # 1) Resolve key fields for the STUB loader:
    #    - If match_fields are ints (based on INPUT header), convert them to NAMES.
    key_fields_cfg = config.get("match_fields", [])
    key_fields_for_loader = key_fields_cfg

    try:
        if key_fields_cfg and isinstance(key_fields_cfg[0], int):
            # Prefer headers from the mapping GUI; fall back to any header we stored.
            mapping = shared_data.get("stub_field_mapping",
                                      {}) if 'shared_data' in globals() or 'shared_data' in locals() else {}
            input_header = (
                    config.get("input_header")
                    or mapping.get("input_header")
                    or shared_data.get("input_header")
                    or shared_data.get("main_header")
            )
            if not input_header:
                raise ValueError("Missing input header to map match_fields indexes to names.")

            key_fields_for_loader = [input_header[i] for i in key_fields_cfg]
            if logger:
                logger.debug(f"[STUB] Resolved key field NAMES for loader: {key_fields_for_loader}")
    except Exception as e:
        if logger:
            logger.warning(f"[STUB] Failed to map match_fields indexes to names: {e}")
        # As a safety net, keep key_fields_for_loader as-is (may still be names)

    # 2) Call the centralized loader (let it sniff the stub delimiter)
    lookup, key_names, stub_names, used_enc, used_delim = load_stub_lookup_table(
        stub_file_path=config.get("stub_file"),
        key_fields=key_fields_for_loader,  # << names (safe for stub header)
        stub_fields=config.get("stub_fields", []),  # ints or names relative to STUB header
        logger=logger,
        delimiter=None,  # let v2 sniff the stub file delimiter
        encoding=None
    )

    # Keep the local name for downstream code that expects it
    stub_lookup = lookup

    # Store for later steps / logging
    config["stub_lookup"] = lookup
    config["key_names"] = key_names
    config["stub_names"] = stub_names
    config["used_encoding"] = used_enc
    config["used_delimiter"] = used_delim

    # ------------------ ADD THESE TWO HELPERS *HERE* ------------------
    def _compute_widths_from_lookup(lookup_dict, key_names_list, log=None):
        widths = {name: 0 for name in key_names_list}
        for composite in lookup_dict.keys():
            parts = str(composite).split("||")
            for idx, part in enumerate(parts):
                if idx < len(key_names_list):
                    s = ("" if part is None else str(part)).strip()
                    if len(s) > widths[key_names_list[idx]]:
                        widths[key_names_list[idx]] = len(s)
        if log:
            log.info(f"[STUB] Stub key widths: {widths}")
        return widths

    def _compute_widths_from_input(input_path, delimiter, encoding, input_header, key_names_list, log=None):
        # local import to avoid top-level dependency if not needed
        import csv
        name_to_idx = {name: input_header.index(name) for name in key_names_list}
        widths = {name: 0 for name in key_names_list}
        with open(input_path, "r", encoding=encoding or "utf-8", newline="") as f:
            r = csv.reader(f, delimiter=delimiter or ",")
            _ = next(r, None)  # skip header (already validated elsewhere)
            for row in r:
                for name in key_names_list:
                    i = name_to_idx[name]
                    s = row[i].strip() if i < len(row) and row[i] is not None else ""
                    if len(s) > widths[name]:
                        widths[name] = len(s)
        if log:
            log.info(f"[INPUT] Input key widths: {widths}")
        return widths

    # ------------------------------------------------------------------

    # Resolve input header (from mapping or config)
    mapping = shared_data.get("stub_field_mapping",
                              {}) if 'shared_data' in globals() or 'shared_data' in locals() else {}
    input_header = (
            config.get("input_header")
            or mapping.get("input_header")
            or shared_data.get("input_header")
            or shared_data.get("main_header")
    )

    main_delim = config.get("main_delimiter", ";")
    input_encoding = shared_data.get("input_encoding") or "utf-8"

    # Compute pad widths as MAX(stub widths, input widths)
    stub_w = _compute_widths_from_lookup(lookup, key_names, logger)
    input_w = _compute_widths_from_input(config["input_file"], main_delim, input_encoding, input_header, key_names,
                                         logger)
    pad_widths = {name: max(stub_w.get(name, 0), input_w.get(name, 0)) for name in key_names}
    config["key_pad_widths"] = pad_widths
    logger.info(f"[MATCH] Using pad widths (max stub ⊕ input): {pad_widths}")

    # Split/normalize composite keys and sort for high–low traversal
    prep = stub_5x_gui.prepare_stub_key_config(
        lookup,
        config["match_fields"],
        logger=logger,
        pad_widths=config["key_pad_widths"],  # <-- pass widths
        key_names=key_names  # <-- and key name order
    )
    config["normalized_stub_lookup"] = prep["normalized_stub_lookup"]
    config["sorted_stub_keys"] = prep["sorted_stub_keys"]

    # --- STEP 2: Read full input into memory (to allow sorting before dispatch) ---
    input_data = []
    with infile:
        reader = csv.reader(infile, delimiter=config["main_delimiter"])
        if config.get("main_has_header"):
            header = next(reader)
            input_data.append(header)
        input_data.extend(reader)

    # --- STEP 3: Sort input records using symmetric normalized key order ---
    data_records = input_data[1:] if config.get("main_has_header") else input_data

    def _norm_key_for_row(row, key_indices, key_names, pad_widths):
        parts = []
        for pos, idx in enumerate(key_indices):
            s = "" if idx >= len(row) or row[idx] is None else str(row[idx]).strip()
            name = key_names[pos] if pos < len(key_names) else None
            w = pad_widths.get(name, 0) if name else 0
            if w and s.isdigit():
                s = s.zfill(w)
            parts.append(s)
        return tuple(parts)

    data_records.sort(
        key=lambda r: _norm_key_for_row(r, config["match_fields"], key_names, config.get("key_pad_widths", {}))
    )

    logger.info("[Stub Logic] Input file sorted by normalized (zero-padded) match key order.")

    logger.info("[Stub Logic] Input file sorted by denormalized match key order.")

    # --- STEP 4: Write output ---
    with outfile:
        # We keep a csv.writer only for emergency fallback paths; normal writes go via _write_record
        writer = csv.writer(outfile, delimiter=config["main_delimiter"])
        # Write our composite (or fallback) header immediately once so even 0-row outputs have a header
        _ensure_header_once(outfile, shared_data, logger)

        if config.get("mode") != "from_file":
            config["normalized_stub_lookup"] = {}
            config["sorted_stub_keys"] = []
            config["stub_index"] = 0

        for i, row in enumerate(data_records):
            total_rows += 1

            results = dispatch_stub_logic(
                record=row,
                config=config,
                record_index=i,
                log_fn=logger
            )

            if not isinstance(results, list):
                results = []

            field_quote_flags = shared_data.get("field_quote_flags", [])

            if results and all(isinstance(r, (list, tuple)) and len(r) == 2 for r in results):
                matched = False
                for updated_row, success in results:
                    if success:
                        # ✅ Reapply original quoting pattern if available
                        quote_map = shared_data.get("field_quote_flags", []) or shared_data.get("quote_map", [])
                        _write_record(outfile, updated_row, config.get("main_delimiter", ";"), quote_map, logger,
                                      shared_data)

                        stub_matches += 1
                        matched = True
                    else:
                        stub_misses += 1
                if not matched:
                    continue
                    # logger.debug(f"[DEBUG] No stub match found for record index {i}")
            else:
                logger.warning(f"[WARNING] No valid results returned for record index {i}")
                stub_misses += 1

    logger.info(f"[SUMMARY] Stub Application Complete")
    logger.info(f"  Records processed: {total_rows}")
    if config.get("mode") == "from_file":
        logger.info(f"  Stub matches:      {stub_matches}")
        logger.info(f"  Records skipped:   {stub_misses}")
    else:
        logger.info(f"  Stub columns inserted: {stub_matches}")

    return output_path

# --- ON COMPLETION - SHOW FINAL STATUS POPUP (non-destructive helper) ---
def show_completion_popup(status: str, function_name: str, message: str = "", parent=None):
    """
    Show a status message. Does NOT destroy any roots or exit the app.
    Pass parent=self.root when calling from the main app.
    """
    from tkinter import messagebox

    title = f"{function_name} " + ("Complete" if status.lower() == "success"
                                   else "Failed" if status.lower() == "failure"
                                   else "Status")

    body = (f"Function '{function_name}' completed successfully.\n\n{message}"
            if status.lower() == "success"
            else f"Function '{function_name}' failed.\n\n{message}"
            if status.lower() == "failure"
            else f"Function '{function_name}' finished with status: {status}\n\n{message}")

    # Show without creating/destroying any extra roots
    if status.lower() == "success":
        messagebox.showinfo(title, body, parent=parent)
    elif status.lower() == "failure":
        messagebox.showerror(title, body, parent=parent)
    else:
        messagebox.showwarning(title, body, parent=parent)

if __name__ == "__main__":
    root = tk.Tk()
    app = FileUtilityApp(root)
    root.mainloop()



