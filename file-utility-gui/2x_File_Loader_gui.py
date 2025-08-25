"""
Program Name: 2x_File_Loader.py
Date Created: 2025-04-02
Last Modified: 2025-04-02

Description:
Handles file I/O operations for the Multi-Function File Utility suite.
Supports reading and writing .csv or .txt files with comma or semicolon delimiters,
and optionally parses a header row if present.

Parameters Sent:
- file_path (str): Path to the input or output file.
- shared_data (dict): Shared data structure with settings.

Parameters Received:
- File content (for reading) or status (for writing).

Return Values:
- read_file() returns list of records (list of lists).
- write_file() returns boolean success status.
- validate_path() returns boolean.

Dependencies:
- os
- csv

Notes / TODOs:
- Expand error handling and fallback behaviors.
- Consider buffering for large file operations.

Change Log:
- 2025-04-02: Initial stub created by ChatGPT
- 2025-04-02: Added support for delimiter, file extensions, and header handling
"""

import os
import csv
from utils_11x_gui import safe_open  # Ensure this import is present

def validate_path(file_path, mode='r'):
    if mode == 'r':
        return os.path.isfile(file_path)
    elif mode == 'w':
        return os.path.isdir(os.path.dirname(file_path)) or os.path.dirname(file_path) == ''
    return False


def detect_delimiter(file_path, shared_data):
    """
    Attempts to auto-detect the delimiter of a CSV file using encoding fallback.
    Returns ',' if detection fails.
    """
    try:
        flex = shared_data.get("flags", {}).get("flexible_decoding", False)
        f, enc, _ = safe_open(file_path, mode='r', newline='', flexible=flex)
        sample = f.read(2048)
        f.close()

        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        return dialect.delimiter
    except Exception as e:
        print(f"[ERROR] Could not auto-detect delimiter: {e}. Defaulting to comma.")
        return ','


def read_file(file_path, shared_data, delimiter=','):
    if not validate_path(file_path, 'r'):
        print(f"[ERROR] File not found: {file_path}")
        return

    print(f"[INFO] Reading file: {file_path} with delimiter '{delimiter}'")

    flex = shared_data.get("flags", {}).get("flexible_decoding", False)
    f, enc, _ = safe_open(file_path, mode='r', newline='', flexible=flex)

    print(f"[INFO] Opened input file using encoding: {enc}")
    with f:
        reader = csv.reader( f, delimiter=delimiter, quotechar='"' )
        chunk_size = shared_data.get( "chunk_size", 1 )
        buffer = []

        for idx, row in enumerate( reader ):
            if idx == 0 and shared_data.get( "has_header" ):
                shared_data["header"] = row
                print( f"[INFO] Header detected: {row}" )
                yield row  # ✅ yield the header so GUI caller can access it
                continue

            if chunk_size == 1:
                yield row
            else:
                buffer.append( row )
                if len( buffer ) >= chunk_size:
                    yield buffer
                    buffer = []

        if chunk_size > 1 and buffer:
            yield buffer


#  Dask-based full file reader (to be used in advanced mode)
def read_file_dask(file_path, shared_data):
    try:
        import dask.dataframe as dd
        if not validate_path(file_path, 'r'):
            print(f"[ERROR] File not found: {file_path}")
            return None

        delimiter = shared_data.get("delimiter", ",")
        has_header = shared_data.get("has_header", False)

        print(f"[INFO] Reading file using Dask: {file_path}")
        df = dd.read_csv(
            file_path,
            sep=delimiter,
            assume_missing=True,
            header=0 if has_header else None,
            dtype=str,
            blocksize="64MB"  # Optional tuning
        )

        # Save header for downstream compatibility
        if has_header:
            shared_data["header"] = list(df.columns)
            print(f"[INFO] Dask-detected header: {shared_data['header']}")

        return df

    except ImportError:
        print("[ERROR] Dask is not installed. Please install with: pip install dask[dataframe]")
        shared_data["errors"].append("Dask not installed.")
        return None

    except Exception as e:
        print(f"[ERROR] Failed to read file with Dask: {e}")
        shared_data["errors"].append("Dask read failed.")
        return None


def write_file(file_path, shared_data, delimiter=','):
    if not validate_path(file_path, 'w'):
        print(f"[ERROR] Invalid write path: {file_path}")
        return None

    print(f"[INFO] Writing to file: {file_path} with delimiter '{delimiter}'")

    output_file = open(file_path, mode='w', newline='', encoding='utf-8')
    writer = csv.writer( output_file, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL )

    if shared_data.get("has_header") and shared_data.get("header"):
        writer.writerow(shared_data["header"])

    # NOTE: Caller is responsible for closing output_file when processing is complete
    return writer, output_file


# Dask-based full file writer
def write_file_dask(df, file_path, shared_data):
    try:
        import dask.dataframe as dd
        if df is None:
            print("[ERROR] No Dask DataFrame to write.")
            return False

        delimiter = shared_data.get("delimiter", ",")
        has_header = shared_data.get("has_header", False)

        print(f"[INFO] Writing Dask DataFrame to: {file_path}")
        df.to_csv(
            file_path,
            sep=delimiter,
            index=False,
            header=has_header,
            single_file=True,
            encoding="utf-8"
        )

        return True

    except ImportError:
        print("[ERROR] Dask is not installed. Please install with: pip install dask[dataframe]")
        shared_data["errors"].append("Dask not installed.")
        return False

    except Exception as e:
        print(f"[ERROR] Failed to write file with Dask: {e}")
        shared_data["errors"].append("Dask write failed.")
        return False
