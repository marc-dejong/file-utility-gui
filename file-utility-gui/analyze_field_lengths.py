def analyze_lengths_from_df(df, col_list, type_list):
    """
    Scans specified columns in the DataFrame to determine:
    - Max string length (after formatting)
    - Decimal precision (for float types)

    Args:
        df (pandas or dask DataFrame): Full input file
        col_list (list[int]): Column indexes to analyze
        type_list (list[str]): Matching types ('int', 'float', 'str')

    Returns:
        (list[int], list[int]): Tuple of (length_list, precision_list)
    """
    length_list = []
    precision_list = []

    for i, col_idx in enumerate(col_list):
        dtype = type_list[i]
        col_name = df.columns[col_idx]
        col = df[col_name].astype(str).str.strip()

        max_length = 0
        max_precision = 0

        for val in col:
            try:
                val_str = str(val).replace("$", "").replace(",", "").replace("(", "-").replace(")", "")
                if dtype == "int":
                    normalized = str(int(float(val_str)))
                elif dtype == "float":
                    normalized = str(float(val_str))
                    if "." in normalized:
                        dec_places = len(normalized.split(".")[1])
                        max_precision = max(max_precision, dec_places)
                else:
                    normalized = val_str

                max_length = max(max_length, len(normalized))

            except Exception:
                continue

        length_list.append(max_length)
        precision_list.append(max_precision if dtype == "float" else 0)

    return length_list, precision_list
