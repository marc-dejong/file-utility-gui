"""
dev_test_12x_batch_normalizer.py

Standalone test harness for normalize_dask_df() from normalizer_12x.py.
Used to validate batch normalization with a small Dask DataFrame.

Author: Marc DeJong
Created: 2025-07-16
"""

import dask.dataframe as dd
import pandas as pd
from normalizer_12x import normalize_dask_df

def main():
    # Sample DataFrame (as list of records)
    data = {
        "ID": ["123", "456", "789"],
        "Amount": ["$12.30", "(45.00)", "67.89"],
        "Notes": ["  abc", "xyz ", "123"]
    }

    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=1)

    # Normalization config
    config = {
        "col_list": [0, 1, 2],
        "field_names": ["ID", "Amount", "Notes"],
        "mode": "batch"
    }


    # Run normalization
    normalized_ddf = normalize_dask_df(ddf, config)
    print("Normalized DataFrame (Dask):")
    print(normalized_ddf.compute())

if __name__ == "__main__":
    main()