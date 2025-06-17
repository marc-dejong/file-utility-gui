# deduper_8x.py
# Deduplication utility module supporting two modes:
# 1. 'dedupe': Keep first occurrence, remove duplicates
# 2. 'keep_duplicates_only': Keep only duplicate records

import dask.dataframe as dd

def dedupe_records(df, dedupe_fields, mode="dedupe"):
    """
    Deduplicates a Dask DataFrame based on specified fields.

    Parameters:
        df (dask.DataFrame): Input DataFrame
        dedupe_fields (list): List of fields to evaluate duplicates on
        mode (str): 'dedupe' or 'keep_duplicates_only'

    Returns:
        dask.DataFrame: Processed DataFrame
    """
    try:
        if not dedupe_fields:
            raise ValueError("No dedupe fields provided.")

        print(f"[8x_Deduper] Mode: {mode}")
        print(f"[8x_Deduper] Fields: {dedupe_fields}")

        # Remove duplicates: Keep first occurrence
        if mode == "dedupe":
            df_result = df.drop_duplicates(subset=dedupe_fields, keep="first")

        # Keep only rows that are duplicated
        elif mode == "keep_duplicates_only":
            # Step 1: Count occurrences of each dedupe key
            group_counts = df.groupby( dedupe_fields ).size().reset_index()
            group_counts = group_counts.rename( columns={0: "_count"} )

            # Step 2: Keep only those with count > 1
            dup_keys = group_counts[group_counts["_count"] > 1]
            dup_keys = dup_keys.drop( "_count", axis=1 )

            # Step 3: Inner join to retain only duplicates
            df_result = df.merge( dup_keys, on=dedupe_fields, how="inner" )


        else:
            raise ValueError(f"Unsupported dedupe mode: {mode}")

        print("[8x_Deduper] Deduplication complete.")
        return df_result

    except Exception as e:
        print(f"[8x_Deduper] ERROR during deduplication: {e}")
        raise
