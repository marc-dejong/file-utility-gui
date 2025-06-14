# sorter_7x.py
# Sorts a DataFrame by one or more fields and writes to CSV (supports Dask DataFrame)

import dask.dataframe as dd

def sort_records(df, output_file, sort_fields, delimiter=","):
    """
    Sorts a Dask DataFrame and writes the sorted result to a CSV file.

    Parameters:
        df (dask.DataFrame): Input Dask DataFrame
        output_file (str): Path to output file
        sort_fields (list): List of column names to sort by
        delimiter (str): Field delimiter to use in output file
    """
    try:
        if not sort_fields:
            raise ValueError("No sort fields provided.")

        print( "\n=== SORT ORDER CONFIRMATION ===" )
        for idx, field in enumerate( sort_fields, start=1 ):
            print( f"[{idx}] {field}" )
        print( "===============================\n" )

        sorted_df = df.sort_values(by=sort_fields)

        sorted_df.to_csv(
            output_file,
            single_file=True,
            index=False,
            sep=delimiter
        )

        print(f"[7x_Sorter] Output written to: {output_file}")

    except Exception as e:
        print(f"[7x_Sorter] ERROR during sort: {e}")
        raise
