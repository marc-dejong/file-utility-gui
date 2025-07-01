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
    import dask.dataframe as dd

    _, f_enc, _ = safe_open( input_file, mode="r", flexible=fallback )
    print( f"[7x_Sorter] Detected encoding: {f_enc}" )
    df = dd.read_csv(
        input_file,
        sep=delimiter,
        encoding=f_enc,
        assume_missing=True,
        dtype=str  # <--- THIS is the generic solution
    )

    try:
        if not sort_fields:
            raise ValueError("No sort fields provided.")

        print( "\n=== SORT ORDER CONFIRMATION ===" )
        for idx, field in enumerate( sort_fields, start=1 ):
            print( f"[{idx}] {field}" )
        print( "===============================\n" )

        import csv  # Ensure this is at the top of the file

        sorted_df = df.sort_values( by=sort_fields )

        if quote_preserve:
            sorted_df.to_csv(
                output_file,
                single_file=True,
                index=False,
                sep=delimiter,
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"'
            )
        else:
            sorted_df.to_csv(
                output_file,
                single_file=True,
                index=False,
                sep=delimiter,
                quoting=csv.QUOTE_NONE,
                quotechar='',
                escapechar='\\'
            )

        print(f"[7x_Sorter] Output written to: {output_file}")

    except Exception as e:
        print(f"[7x_Sorter] ERROR during sort: {e}")
        raise
