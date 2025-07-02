# sorter_7x.py
# Sorts a DataFrame by one or more fields and writes to CSV (supports Dask DataFrame)

import dask.dataframe as dd
import csv

def sort_records(input_file, output_file, sort_fields, delimiter=",", quote_preserve=True, fallback=True):
    """
        Sorts a Dask DataFrame and writes the sorted result to a CSV file.

        Parameters:
            input_file (str): Path to input file
            output_file (str): Path to output file
            sort_fields (list): List of column names to sort by
            delimiter (str): Field delimiter to use in output file
            quote_preserve (bool): If True, retain double quotes in output
            fallback (bool): If True, enable flexible encoding fallback
        """

    from utils_11x_gui import safe_open

    print( f"[7x_Sorter] after import 22 - quote_preserve received: {quote_preserve}" )

    # Determine encoding
    _, f_enc, _ = safe_open( input_file, mode="r", flexible=fallback )
    print( f"[7x_Sorter] Detected encoding: {f_enc}" )

    # ---- MODE 1: QUOTE PRESERVE (NO STRIP) ----
    if quote_preserve:
        print( "[7x_Sorter] Preserving original quotes using raw line sort." )

        # Read lines directly to preserve formatting
        with open( input_file, mode="r", encoding=f_enc, newline="" ) as f:
            header_line = f.readline()
            header_fields = header_line.strip().split( delimiter )
            data_lines = f.readlines()

        # Map sort_fields to column indices
        sort_indices = []
        for field in sort_fields:
            if field not in header_fields:
                raise ValueError( f"[7x_Sorter] Field not found in header: {field}" )
            sort_indices.append( header_fields.index( field ) )

        print( "\n=== SORT ORDER CONFIRMATION ===" )
        for i, field in enumerate( sort_fields, 1 ):
            print( f"[{i}] {field}" )
        print( "===============================\n" )

        def extract_sort_keys(line):
            fields = line.strip().split( delimiter )
            return tuple( fields[i] for i in sort_indices )

        # Perform the sort based on extracted keys
        sorted_lines = sorted( data_lines, key=extract_sort_keys )

        with open( output_file, mode="w", encoding="utf-8", newline="" ) as f:
            f.write( header_line )
            f.writelines( sorted_lines )

        print( f"[7x_Sorter] Output written to: {output_file}" )
        return

    # ---- MODE 2: STRIP QUOTES USING DASK ----
    print( "[7x_Sorter] - quote_preserve received: False" )

    # Load with Dask for parallel sort; dtype=str to ensure consistent column handling
    df = dd.read_csv(
        input_file,
        sep=delimiter,
        encoding=f_enc,
        assume_missing=True,
        dtype=str
    )

    print( "\n=== SORT ORDER CONFIRMATION ===" )
    for i, field in enumerate( sort_fields, 1 ):
        print( f"[{i}] {field}" )
    print( "===============================\n" )

    sorted_df = df.sort_values( by=sort_fields )

    # Save with quotes stripped (QUOTE_NONE), and escapechar to avoid crash on quote-containing values
    sorted_df.to_csv(
        output_file,
        single_file=True,
        index=False,
        sep=delimiter,
        quoting=csv.QUOTE_NONE,
        quotechar='"',
        escapechar='\\'  # <-- REQUIRED for QUOTE_NONE
    )

    print( f"[7x_Sorter] Output written to: {output_file}" )

    return None

