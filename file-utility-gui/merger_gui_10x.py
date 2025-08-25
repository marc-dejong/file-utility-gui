"""
MODULE: 10x_Merger.py

DESCRIPTION:
    This module handles record-level filtering or merging based on a secondary key file.
    It supports two primary operations:
    - Match Mode: Retain only input file records that match values in a key file.
    - Merge Mode: Retain and append selected fields (or all fields) from key file records.

    Matching supports multi-column keys and automatic normalization (e.g., stripping
    leading zeros from fields like BPI). Matching can be done by header name or column index.

    The user provides:
    - Paths and delimiters for both input and key files
    - Header presence flags
    - Column mappings between the two files
    - Merge behavior (filter only or append fields)

    All configuration is driven by user prompts, with no GUI required.

DEPENDENCIES:
    - utils_11x.py (for file prompts, yes/no input, and column display)

LIMITATIONS:
    - Only supports matching one input file at a time
    - Merge mode performs a linear scan for each match (Phase 2 could cache key file)

AUTHOR:
    Oliver / MARC DEJONG, 2025
"""
import csv
import tkinter as tk
from tkinter import messagebox, simpledialog

from normalizer_core import normalize_record

from utils_11x_gui import (
    prompt_for_file_path,
    prompt_for_delimiter,
    prompt_yes_no,
    display_column_choices,
    safe_open,
)

def gui_select_fields(title, header_list):
    win = tk.Toplevel()
    win.title(title)
    win.geometry("450x500")
    selected_fields_order = []

    label = tk.Label(win, text="Select field(s) from list OR enter manually below:")
    label.pack(pady=(10, 5))

    indexed_fields = [f"{i}: {name}" for i, name in enumerate(header_list)]
    listbox = tk.Listbox(win, selectmode=tk.MULTIPLE, exportselection=False, height=15)
    for item in indexed_fields:
        listbox.insert(tk.END, item)
    listbox.pack(pady=(0, 10))

    def on_submit():
        selection = listbox.curselection()
        selected_fields_order.extend([int(indexed_fields[i].split(":")[0]) for i in selection])
        win.destroy()

    submit_btn = tk.Button(win, text="Confirm Selection", command=on_submit)
    submit_btn.pack(pady=10)

    win.grab_set()
    win.wait_window()

    return selected_fields_order

def display_column_choices(headers, logger=None):
    for i, name in enumerate(headers):
        msg = f"[{i}] {name}"
        print(msg)
        if logger:
            logger.info(msg)

def append_fields_to_record(main_parts, key_parts, append_fields, delimiter, logger=None):
    """
    Combines a main record with selected fields from the key record.

    Args:
        main_parts (list[str]): The original input record split by delimiter.
        key_parts (list[str]): The matching key record split by delimiter.
        append_fields (list[int]): Indexes of fields to pull from key_parts.
        delimiter (str): Delimiter for output.
        logger (logging.Logger, optional): Logger instance for logging (if any).

    Returns:
        str: A single merged line with fields joined by the delimiter.
    """
    try:
        append_vals = [key_parts[i] for i in append_fields]
        combined = main_parts + append_vals
        merged_line = delimiter.join(combined)

        if logger:
            logger.debug(f"[MERGE] Appended fields: {append_vals}")
            logger.debug(f"[MERGE] Final combined record: {merged_line}")

        return merged_line
    except Exception as e:
        if logger:
            logger.error(f"[ERROR] Failed to append fields: {e}")
        raise


def prompt_for_merge_config(logger=None):

    if logger is None:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("merge_config_fallback")

    config = {}

    # MAIN input file
    config["main_file"] = prompt_for_file_path(
        "Enter the full path to your MAIN input file (the file you want to filter or merge):"
    )
    config["main_delimiter"] = prompt_for_delimiter("Enter the delimiter for the MAIN input file:")
    config["main_has_header"] = prompt_yes_no("Does the MAIN input file contain a header record?")

    # KEY file
    config["key_file"] = prompt_for_file_path("Enter the full path to your KEY file:")
    config["key_delimiter"] = prompt_for_delimiter("Enter the delimiter for the KEY file:")
    config["key_has_header"] = prompt_yes_no("Does the KEY file contain a header record?")

    # Match or Merge
    while True:
        mode = simpledialog.askstring(
            "Match Mode",
            "Choose how to handle matched records:\n\n"
            "1 - Keep only matching records from MAIN file\n"
            "2 - Merge: Append fields from KEY file to MAIN file"
        )
        if mode in ("1", "2"):
            config["merge_mode"] = int(mode)
            break
        messagebox.showerror("Invalid Selection", "Please enter 1 or 2.")

    return config


def prompt_for_field_mapping(key_headers, main_headers, has_key_header, has_main_header):
    """
    Prompts the user to map fields from the KEY file to the MAIN file using a GUI.
    Returns a list of (key_idx, main_idx) pairs.
    """
    # Step 1: Attempt auto-mapping by header name
    if has_key_header and has_main_header:
        auto_mapping = []
        for key_idx, key_name in enumerate(key_headers):
            if key_name in main_headers:
                main_idx = main_headers.index(key_name)
                auto_mapping.append((key_idx, main_idx))

        if auto_mapping:
            preview = "\n".join(
                f"KEY[{key_idx}] {key_headers[key_idx]} ↔ MAIN[{main_idx}] {main_headers[main_idx]}"
                for key_idx, main_idx in auto_mapping
            )
            use_auto = messagebox.askyesno(
                "Auto Field Mapping Found",
                f"The following column matches were found:\n\n{preview}\n\nUse this mapping?"
            )
            if use_auto:
                return auto_mapping

    # Step 2: Manual mapping via GUI listboxes
    win = tk.Toplevel()
    win.title("Map Fields from KEY to MAIN")
    win.geometry("700x500")
    mappings = []

    tk.Label(win, text="Select field from KEY file").grid(row=0, column=0, padx=10, pady=10)
    tk.Label(win, text="Select corresponding field from MAIN file").grid(row=0, column=1, padx=10, pady=10)

    key_listbox = tk.Listbox(win, height=20, exportselection=False)
    main_listbox = tk.Listbox(win, height=20, exportselection=False)

    for i, name in enumerate(key_headers):
        key_listbox.insert(tk.END, f"{i}: {name}")
    for i, name in enumerate(main_headers):
        main_listbox.insert(tk.END, f"{i}: {name}")

    key_listbox.grid(row=1, column=0, padx=10)
    main_listbox.grid(row=1, column=1, padx=10)

    output_label = tk.Label(win, text="Mappings:")
    output_label.grid(row=2, column=0, columnspan=2, pady=(20, 0))

    result_list = tk.Listbox(win, width=60)
    result_list.grid(row=3, column=0, columnspan=2, padx=10, pady=5)

    def add_mapping():
        key_sel = key_listbox.curselection()
        main_sel = main_listbox.curselection()
        if not key_sel or not main_sel:
            messagebox.showerror("Selection Error", "Please select both KEY and MAIN fields.")
            return
        key_idx = int(key_listbox.get(key_sel[0]).split(":")[0])
        main_idx = int(main_listbox.get(main_sel[0]).split(":")[0])
        mappings.append((key_idx, main_idx))
        result_list.insert(tk.END, f"KEY[{key_idx}] {key_headers[key_idx]} ↔ MAIN[{main_idx}] {main_headers[main_idx]}")

    def on_done():
        if not mappings:
            messagebox.showerror("No Mappings", "At least one field mapping is required.")
            return
        win.destroy()

    tk.Button(win, text="Add Mapping", command=add_mapping).grid(row=4, column=0, pady=10)
    tk.Button(win, text="Done", command=on_done).grid(row=4, column=1, pady=10)

    win.grab_set()
    win.wait_window()

    return mappings


def prompt_for_key_fields_to_append(key_headers):
    choice = simpledialog.askstring(
        "Append Fields Mode",
        "Select fields from KEY file to append to output:\n\n"
        "1 - Append ALL fields from KEY file\n"
        "2 - Select specific fields to append"
    )
    if choice == "1":
        return list(range(len(key_headers)))
    elif choice == "2":
        return gui_select_fields("Select fields to append", key_headers)
    else:
        messagebox.showerror("Invalid Choice", "Please enter 1 or 2.")
        return prompt_for_key_fields_to_append(key_headers)  # Retry


def build_key_set(key_file_path, delimiter, has_header, mapping, logger=None):
    if logger is None:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("merge_keyset_fallback")

    key_set = set()
    f, f_enc, _ = safe_open(key_file_path, mode="r")
    msg = f"[INFO] Opened Key File in build_key_set using encoding: {f_enc}"
    print(msg)
    if logger:
        logger.info(msg)

    key_fields_keyfile = [m[0] for m in mapping]
    for line_num, line in enumerate(f):
        if line_num == 0 and has_header:
            continue

        kparts = line.strip().split(delimiter)
        kkey = extract_normalized_key(kparts, key_fields_keyfile, logger=logger)
        key_set.add(kkey)

    f.close()
    return key_set

def extract_normalized_key(record, field_indices, logger=None):
    """
    Given a record (list of fields), return a normalized tuple key
    using specified field indices.

    Args:
        record (list[str]): The full input line split by delimiter.
        field_indices (list[int]): Indexes of fields to normalize.
        logger (optional): Logger instance.

    Returns:
        tuple: Normalized key tuple
    """
    try:
        input_vals = [record[i] for i in field_indices]
        config = {
            "input_value_list": input_vals,
            "col_list": field_indices,
            "field_names": [f"col_{i}" for i in field_indices],
            "mode": "record"
        }
        result = normalize_record(config)
        return tuple(result["output_value_list"])
    except Exception as e:
        if logger:
            logger.error(f"[ERROR] Failed to normalize record: {e}")
        raise



def filter_and_merge_main_file(config, mapping, append_fields, shared_data, logger=None):

    if logger is None:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("merge_filter_fallback")

    shared_data["records_read"] = 0
    shared_data["records_written"] = 0

    output_path = simpledialog.askstring(
        "Output File Path",
        "Enter full path for the output file:"
    )

    if not output_path:
        messagebox.showerror("Missing Output Path", "No output file path was provided. Operation cancelled.")
        return

    write_header = messagebox.askyesno(
        "Include Header?",
        "Should the output file include a header row?"
    )

    key_tuples = build_key_set(config["key_file"], config["key_delimiter"], config["key_has_header"], mapping)

    fin, fin_enc, _ = safe_open( config["main_file"], mode="r" )
    fout, fout_enc, _ = safe_open( output_path, mode="w", newline="" )

    logger.info(f"[INFO] Opened Main File using encoding: {fin_enc}")
    logger.info(f"[INFO] Opened Output File using encoding: {fout_enc}")

    with fin, fout:
        if config["main_has_header"]:
            main_header = fin.readline().rstrip('\n').split(config["main_delimiter"])
            if write_header:
                header_line = config["main_delimiter"].join(main_header)
                if config["merge_mode"] == 2:
                    fkey, fkey_enc, _ = safe_open(config["key_file"], mode="r")
                    logger.info(f"[INFO] Opened Key File using encoding: {fkey_enc}")
                    with fkey:
                        key_header = fkey.readline().rstrip('\n').split(config["key_delimiter"])
                        append_headers = [key_header[i] for i in append_fields]
                        header_line += config["main_delimiter"] + config["main_delimiter"].join(append_headers)
                fout.write(header_line + '\n')

        for line in fin:
            shared_data["records_read"] += 1
            main_parts = line.rstrip('\n').split(config["main_delimiter"])
            key_fields_main = [m[1] for m in mapping]
            main_key = extract_normalized_key(main_parts, key_fields_main, logger=logger)

            if main_key in key_tuples:
                shared_data["records_written"] += 1

                if config["merge_mode"] == 1:
                    fout.write(line)

                elif config["merge_mode"] == 2:
                    fkey, fkey_enc, _ = safe_open(config["key_file"], mode="r")
                    logger.info(f"[INFO] Re-opened Key File for match search using encoding: {fkey_enc}")

                    with fkey:
                        if config["key_has_header"]:
                            next(fkey)

                        for kline in fkey:
                            kparts = kline.rstrip('\n').split(config["key_delimiter"])
                            key_fields_keyfile = [m[0] for m in mapping]

                            kkey = extract_normalized_key(kparts, key_fields_keyfile, logger=logger)

                            if kkey == main_key:
                                merged_line = append_fields_to_record(
                                    main_parts, kparts, append_fields, config["main_delimiter"], logger=logger
                                )
                                fout.write(merged_line + '\n')
                                break


def run_merge_10x_process(shared_data=None, mode=None):

    if shared_data is None:
        shared_data = {}

    # Setup logger from shared_data or fallback
    logger = shared_data.get("logger")
    if logger is None:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("merger_fallback")

    msg = "\n< - - - - MERGE MODULE STARTING - - - - >\n"
    print(msg)
    logger.info(msg)

    msg = f"[DEBUG] merge_10x mode: {mode}"
    print(msg)
    logger.debug(msg)

    msg = f"[DEBUG] input_file_1: {shared_data.get('input_file')}"
    print(msg)
    logger.debug(msg)

    msg = f"[DEBUG] input_file_2: {shared_data.get('second_file')}"
    print(msg)
    logger.debug(msg)

    msg = f"[DEBUG] output_file  : {shared_data.get('output_file')}"
    print(msg)
    logger.debug(msg)

    msg = f"[DEBUG] delimiter    : {shared_data.get('delimiter')}"
    print(msg)
    logger.debug(msg)

    if mode == "basic":
        logger.info(f"[INFO] GUI-triggered mode: {mode}")
        shared_data["status"] = "Starting Basic Merge"

        # Get input/output files and delimiter from shared_data
        input_file_1 = shared_data.get("input_file")
        input_file_2 = shared_data.get("second_file")
        delimiter = shared_data.get("delimiter", ",")
        has_header = shared_data.get("has_header", True)
        output_file = shared_data.get("output_file")

        if not all([input_file_1, input_file_2, output_file]):
            print("[ERROR] Missing file paths from GUI input.")
            logger.error("[ERROR] Missing file paths from GUI input.")
            messagebox.showerror("Merge Error", "One or more required files are missing.")
            return

        # Now run basic merge
        try:
            f1, enc1, _ = safe_open( input_file_1, mode="r", newline="" )
            f2, enc2, _ = safe_open( input_file_2, mode="r", newline="" )
            fout, enc_out, _ = safe_open( output_file, mode="w", newline="" )

            logger.info( f"[INFO] Opened File 1 using encoding: {enc1}" )
            logger.info( f"[INFO] Opened File 2 using encoding: {enc2}" )
            logger.info( f"[INFO] Opened Output File using encoding: {enc_out}" )

            with f1, f2, fout:
                try:
                    rows1 = list( csv.reader( f1, delimiter=delimiter ) )
                except UnicodeDecodeError as e:
                    print( f"[FATAL] Failed to read File 1 due to encoding issue: {e}" )
                    logger.error( f"[FATAL] Failed to read File 1 due to encoding issue: {e}" )
                    messagebox.showerror( "Read Error", f"Failed to read File 1 due to encoding error:\n{e}" )
                    return
                except Exception as e:
                    print( f"[FATAL] Unexpected error reading File 1: {e}" )
                    logger.error( f"[FATAL] Unexpected error reading File 1: {e}" )
                    messagebox.showerror( "Read Error", f"Unexpected error while reading File 1:\n{e}" )
                    return

                try:
                    rows2 = list( csv.reader( f2, delimiter=delimiter ) )
                except UnicodeDecodeError as e:
                    print( f"[FATAL] Failed to read File 2 due to encoding issue: {e}" )
                    logger.error( f"[FATAL] Failed to read File 2 due to encoding issue: {e}" )
                    messagebox.showerror( "Read Error", f"Failed to read File 2 due to encoding error:\n{e}" )
                    return
                except Exception as e:
                    print( f"[FATAL] Unexpected error reading File 2: {e}" )
                    logger.error( f"[FATAL] Unexpected error reading File 2: {e}" )
                    messagebox.showerror( "Read Error", f"Unexpected error while reading File 2:\n{e}" )
                    return

                if not rows1:
                    raise ValueError( "First input file appears to be empty." )
                if not rows2:
                    raise ValueError( "Second input file appears to be empty." )

                strip_quotes = shared_data.get( "flags", {} ).get( "strip_quotes", False )

                if strip_quotes:
                    quoting = csv.QUOTE_NONE
                    quotechar = ''
                    escapechar = '\\'
                else:
                    quoting = csv.QUOTE_NONNUMERIC
                    quotechar = '"'
                    escapechar = None

                writer = csv.writer(
                    fout,
                    delimiter=delimiter,
                    quoting=quoting,
                    quotechar=quotechar,
                    escapechar=escapechar
                )

                if has_header:
                    header1 = rows1[0]
                    header2 = rows2[0]

                    if header1 != header2:
                        logger.warning(
                            f"[WARN] Header mismatch between files.\nFile 1 Header: {header1}\nFile 2 Header: {header2}")

                    writer.writerow(header1)
                    rows1 = rows1[1:]  # always remove first header

                    # Only drop second header if it matches the first
                    if header2 == header1:
                        logger.info("[INFO] Header row detected in File 2 — removing.")
                        rows2 = rows2[1:]
                    else:
                        logger.info("[INFO] No matching header in File 2 — retaining all rows.")

                count1 = 0
                count2 = 0

                # Write data
                for row in rows1:
                    if any( cell.strip() for cell in row ):
                        writer.writerow( row )
                        count1 += 1

                for row in rows2:
                    if any( cell.strip() for cell in row ):
                        writer.writerow( row )
                        count2 += 1

                print( f"[INFO] Rows written from File 1: {count1:,}" )
                print( f"[INFO] Rows written from File 2: {count2:,}" )
                print( f"[INFO] Total records written (excluding header): {count1 + count2:,}" )
                logger.info( f"[INFO] Rows written from File 1: {count1:,}" )
                logger.info( f"[INFO] Rows written from File 2: {count2:,}" )
                logger.info( f"[INFO] Total records written (excluding header): {count1 + count2:,}" )
                summary = (
                    f"Rows written from File 1: {count1:,}\n"
                    f"Rows written from File 2: {count2:,}\n"
                    f"Total records written (excluding header): {count1 + count2:,}"
                )
                messagebox.showinfo( "Concatenation Complete", summary )

            response = messagebox.askyesno( "Next Step", "Do you want to process another file?" )
            if not response:
                shared_data["root"].destroy()

        except Exception as io_err:
            logger.error( f"[ERROR] Merge failed during read: {io_err}" )
            messagebox.showerror( "Merge Error", f"Merge failed during read:\n{io_err}" )

    elif mode == "key":
        logger.info( "[MODE] Running Key-Based Merge from GUI" )

        input_file = shared_data.get( "input_file" )
        key_file = shared_data.get( "second_file" )
        output_file = shared_data.get( "output_file" )
        has_header = shared_data.get( "has_header", True )
        delimiter = shared_data.get( "delimiter", "," )

        key_fields_input = shared_data.get( "key_fields" )
        key_fields_keyfile = shared_data.get( "keyfile_key_fields" )

        if not input_file or not key_file or not output_file:
            logger.error( "[ERROR] One or more file paths are missing." )
            messagebox.showerror( "Merge by Key Error", "Input, key, or output file path is missing." )
            return

        if not key_fields_input or not key_fields_keyfile:
            logger.error( "[ERROR] Key field selections are missing." )
            messagebox.showerror( "Merge by Key Error", "Key field mappings from GUI input are missing." )
            return

        try:
            # Step 1: Build key set from key file
            key_set = set()
            kf, kf_enc, _ = safe_open( key_file, mode="r" )
            logger.info( f"[INFO] Opened Key File using encoding: {kf_enc}" )

            with kf:
                reader = csv.reader(kf, delimiter=delimiter)
                if has_header:
                    next(reader, None)
                for row in reader:
                    if max(key_fields_keyfile) >= len(row):
                        continue  # Skip short or malformed rows
                    key = extract_normalized_key(row, key_fields_keyfile, logger=logger)
                    key_set.add(key)

            # Step 2: Open input file and write matches to output
            match_count = 0
            inf, inf_enc, _ = safe_open( input_file, mode="r" )
            logger.info( f"[INFO] Opened Input File using encoding: {inf_enc}" )
            outf, outf_enc, _ = safe_open( output_file, mode="w", newline="" )
            logger.info( f"[INFO] Opened Output File using encoding: {outf_enc}" )

            with inf, outf:
                reader = csv.reader( inf, delimiter=delimiter )
                strip_quotes = shared_data.get( "flags", {} ).get( "strip_quotes", False )

                if strip_quotes:
                    quoting = csv.QUOTE_NONE
                    quotechar = ''
                    escapechar = '\\'
                else:
                    quoting = csv.QUOTE_NONNUMERIC
                    quotechar = '"'
                    escapechar = None

                writer = csv.writer(
                    outf,
                    delimiter=delimiter,
                    quoting=quoting,
                    quotechar=quotechar,
                    escapechar=escapechar
                )

                if has_header:
                    header = next( reader, None )
                    writer.writerow( header )

                for row in reader:
                    if max( key_fields_input ) >= len( row ):
                        continue
                    main_input_vals = [row[i] for i in key_fields_input]

                    config_norm = {
                        "input_value_list": main_input_vals,
                        "col_list": key_fields_input,
                        "field_names": [f"col_{i}" for i in key_fields_input],
                        "mode": "record"
                    }

                    result = normalize_record(config_norm)
                    main_key = tuple(result["output_value_list"])

                    if main_key in key_set:
                        writer.writerow( row )
                        match_count += 1

            logger.info( f"[SUCCESS] Merge by key complete. {match_count} matching records written." )
            print( f"[SUCCESS] Merge by key complete. {match_count} matching records written." )
            shared_data["merge_successful"] = True
            shared_data["status"] = f"{match_count} records matched and written to {output_file}"
            messagebox.showinfo( "Merge Complete", f"{match_count} records matched and written." )
            response = messagebox.askyesno( "Next Step", "Do you want to process another file?" )
            if not response:
                shared_data["root"].destroy()

        except Exception as e:
            print( f"[ERROR] Merge by key failed: {e}" )
            logger.error( f"[ERROR] Merge by key failed: {e}" )
            import traceback
            traceback.print_exc()
            logger.error("An error occurred:\n" + traceback.format_exc())
            messagebox.showerror( "Merge Error", f"Merge by key failed:\n{e}" )
            return

if __name__ == "__main__":
    run_merge_10x_process( {} )


