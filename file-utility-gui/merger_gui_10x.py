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
import traceback

from tkinter import messagebox

def normalize_field(value: str) -> str:
    return value.strip().strip('"').lstrip('0') or '0'


def display_column_choices(headers):
    for i, name in enumerate(headers):
        print(f"[{i}] {name}")


def prompt_for_merge_config():
    from utils_11x_gui import prompt_for_file_path, prompt_for_delimiter, prompt_yes_no, display_column_choices

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
    print("\nChoose how to handle matched records:")
    print("1 - Keep only matching records from MAIN file")
    print("2 - Merge: Append fields from KEY file to MAIN file")
    while True:
        mode = input("Enter choice (1 or 2): ").strip()
        if mode in ("1", "2"):
            config["merge_mode"] = int(mode)
            break
        print("[ERROR] Invalid selection. Choose 1 or 2.")

    return config


def prompt_for_field_mapping(key_headers, main_headers, has_key_header, has_main_header):
    print("\nEnter column mappings between the KEY file and MAIN input file.")
    print("Format examples:")
    if has_key_header and has_main_header:
        print("  BPI:BPI, Year:Year")
    else:
        print("  2:4, 3:5")

    print("\nAvailable KEY columns:")
    display_column_choices(key_headers)

    print("\nAvailable MAIN columns:")
    display_column_choices(main_headers)

    # Attempt automatic mapping if both files have headers
    if has_key_header and has_main_header:
        auto_mapping = []
        for key_idx, key_col in enumerate( key_headers ):
            if key_col in main_headers:
                main_idx = main_headers.index( key_col )
                auto_mapping.append( (key_idx, main_idx) )

        if auto_mapping:
            print( "\nAuto-matched the following columns:" )
            for key_idx, main_idx in auto_mapping:
                print( f"  KEY[{key_idx}] {key_headers[key_idx]} ↔ MAIN[{main_idx}] {main_headers[main_idx]}" )
            confirm = input( "Use this mapping? (y/n): " ).strip().lower()
            if confirm in {"y", "yes"}:
                return auto_mapping
            else:
                print( "Proceeding to manual mapping...\n" )

    while True:
        raw = input( "Enter mapping (comma-separated key:main pairs): " ).strip()

        try:
            mappings = []

            for pair in raw.split( ',' ):
                left, right = pair.strip().split( ':' )

                try:
                    # Try numeric mapping (index-based)
                    key_index = int( left.strip() )
                    main_index = int( right.strip() )
                except ValueError:
                    # Fallback to name-based mapping
                    if has_key_header and has_main_header:
                        key_index = key_headers.index( left.strip() )
                        main_index = main_headers.index( right.strip() )
                    else:
                        raise ValueError( "Column names used but headers are missing." )

                mappings.append( (key_index, main_index) )

            return mappings

        except Exception as e:
            print( f"[ERROR] Invalid format: {e}" )


def prompt_for_key_fields_to_append(key_headers):
    print("\nSelect fields from KEY file to append to the output.")
    print("1 - Append ALL fields from KEY file")
    print("2 - Append selected fields only")
    while True:
        choice = input("Enter choice (1 or 2): ").strip()
        if choice == '1':
            return list(range(len(key_headers)))  # all fields
        elif choice == '2':
            display_column_choices(key_headers)
            raw = input("Enter column numbers (comma-separated): ").strip()
            try:
                indices = [int(i.strip()) for i in raw.split(',')]
                return indices
            except Exception as e:
                print(f"[ERROR] Invalid input: {e}")
        else:
            print("[ERROR] Please enter 1 or 2.")


def build_key_set(key_file_path, delimiter, has_header, mapping):
    key_set = set()
    with open(key_file_path, encoding='utf-8') as f:
        lines = f.readlines()
        if has_header:
            lines = lines[1:]
        for line in lines:
            parts = line.rstrip("\n").split(delimiter)
            key = tuple(normalize_field(parts[int(k)]) for k, _ in mapping)
            key_set.add(key)
    return key_set


def filter_and_merge_main_file(config, mapping, append_fields, shared_data):
    shared_data["records_read"] = 0
    shared_data["records_written"] = 0

    output_path = input("\nEnter full path for output file: ").strip()
    write_header = input("Should the output include a header? (y/n): ").lower().startswith("y")

    key_tuples = build_key_set(config["key_file"], config["key_delimiter"], config["key_has_header"], mapping)

    with open(config["main_file"], encoding='utf-8') as fin, \
         open(output_path, 'w', encoding='utf-8') as fout:

        if config["main_has_header"]:
            main_header = fin.readline().rstrip('\n').split(config["main_delimiter"])
            if write_header:
                header_line = config["main_delimiter"].join(main_header)
                if config["merge_mode"] == 2:
                    with open(config["key_file"], encoding='utf-8') as fkey:
                        key_header = fkey.readline().rstrip('\n').split(config["key_delimiter"])
                        append_headers = [key_header[i] for i in append_fields]
                        header_line += config["main_delimiter"] + config["main_delimiter"].join(append_headers)
                fout.write(header_line + '\n')

        for line in fin:
            shared_data["records_read"] += 1
            main_parts = line.rstrip('\n').split(config["main_delimiter"])
            main_key = tuple(normalize_field(main_parts[m[1]]) for m in mapping)

            if main_key in key_tuples:
                shared_data["records_written"] += 1
                if config["merge_mode"] == 1:
                    fout.write(line)
                elif config["merge_mode"] == 2:
                    with open(config["key_file"], encoding='utf-8') as fkey:
                        if config["key_has_header"]:
                            next(fkey)
                        for kline in fkey:
                            kparts = kline.rstrip('\n').split(config["key_delimiter"])
                            kkey = tuple(normalize_field(kparts[m[0]]) for m in mapping)
                            if kkey == main_key:
                                append_vals = [kparts[i] for i in append_fields]
                                combined = main_parts + append_vals
                                fout.write(config["main_delimiter"].join(combined) + '\n')
                                break

def run_merge_10x_process(shared_data=None, mode=None):

    if shared_data is None:
        shared_data = {}

    print("\n< - - - - MERGE MODULE STARTING - - - - >\n")

    if mode == "basic":
        print(f"[INFO] GUI-triggered mode: {mode}")
        shared_data["status"] = "Starting Basic Merge"

        # Get input/output files and delimiter from shared_data
        input_file_1 = shared_data.get("input_file")
        input_file_2 = shared_data.get("second_file")
        delimiter = shared_data.get("delimiter", ",")
        has_header = shared_data.get("has_header", True)
        output_file = shared_data.get("output_file")

        if not all([input_file_1, input_file_2, output_file]):
            print("[ERROR] Missing file paths from GUI input.")
            messagebox.showerror("Merge Error", "One or more required files are missing.")
            return

        # Now run basic merge
        try:
            with open(input_file_1, "r", newline="", encoding="utf-8") as f1, \
                 open(input_file_2, "r", newline="", encoding="utf-8") as f2, \
                 open(output_file, "w", newline="", encoding="utf-8") as fout:

                reader1 = csv.reader(f1, delimiter=delimiter)
                reader2 = csv.reader(f2, delimiter=delimiter)
                writer = csv.writer(fout, delimiter=delimiter)

                if has_header:
                    header1 = next( reader1, [] )
                    next( reader2, None )  # Skip second file’s header
                    writer.writerow( header1 )

                count1 = 0
                count2 = 0

                for row in reader1:
                    writer.writerow( row )
                    count1 += 1

                for row in reader2:
                    writer.writerow( row )
                    count2 += 1

                print( f"[INFO] Rows written from File 1: {count1:,}" )
                print( f"[INFO] Rows written from File 2: {count2:,}" )
                print( f"[INFO] Total records written (excluding header): {count1 + count2:,}" )

                response = messagebox.askyesno( "Next Step", "Do you want to process another file?" )
                if not response:
                    shared_data["root"].destroy()

        except Exception as e:
            print(f"[ERROR] Merge failed: {e}")
            messagebox.showerror("Merge Error", f"Merge failed:\n{e}")
        return

    elif mode == "key":
        print( "[MODE] Running Key-Based Merge from GUI" )

        input_file = shared_data.get( "input_file" )
        key_file = shared_data.get( "second_file" )
        output_file = shared_data.get( "output_file" )
        has_header = shared_data.get( "has_header", True )
        delimiter = shared_data.get( "delimiter", "," )

        key_fields_input = shared_data.get( "key_fields" )
        key_fields_keyfile = shared_data.get( "keyfile_key_fields" )

        if not input_file or not key_file or not output_file:
            print( "[ERROR] One or more file paths are missing." )
            messagebox.showerror( "Merge by Key Error", "Input, key, or output file path is missing." )
            return

        if not key_fields_input or not key_fields_keyfile:
            print( "[ERROR] Key field selections are missing." )
            messagebox.showerror( "Merge by Key Error", "Key field mappings from GUI input are missing." )
            return

        try:
            # Step 1: Build key set from key file
            key_set = set()
            with open( key_file, "r", encoding="utf-8" ) as kf:
                reader = csv.reader( kf, delimiter=delimiter )
                if has_header:
                    next( reader, None )
                for row in reader:
                    if max( key_fields_keyfile ) >= len( row ):
                        continue  # Skip short or malformed rows
                    key = tuple( row[i].strip().lstrip( "0" ) for i in key_fields_keyfile )
                    key_set.add( key )

            # Step 2: Open input file and write matches to output
            match_count = 0
            with open( input_file, "r", encoding="utf-8" ) as inf, \
                    open( output_file, "w", newline="", encoding="utf-8" ) as outf:

                reader = csv.reader( inf, delimiter=delimiter )
                writer = csv.writer( outf, delimiter=delimiter )

                if has_header:
                    header = next( reader, None )
                    writer.writerow( header )

                for row in reader:
                    if max( key_fields_input ) >= len( row ):
                        continue
                    key = tuple( row[i].strip().lstrip( "0" ) for i in key_fields_input )
                    if key in key_set:
                        writer.writerow( row )
                        match_count += 1

            print( f"[SUCCESS] Merge by key complete. {match_count} matching records written." )
            shared_data["merge_successful"] = True
            shared_data["status"] = f"{match_count} records matched and written to {output_file}"
            messagebox.showinfo( "Merge Complete", f"{match_count} records matched and written." )
            response = messagebox.askyesno( "Next Step", "Do you want to process another file?" )
            if not response:
                shared_data["root"].destroy()



        except Exception as e:
            print( f"[ERROR] Merge by key failed: {e}" )
            traceback.print_exc()
            messagebox.showerror( "Merge Error", f"Merge by key failed:\n{e}" )
            return

if __name__ == "__main__":
    run_merge_10x_process( {} )


