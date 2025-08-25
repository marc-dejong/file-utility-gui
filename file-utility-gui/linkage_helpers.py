import dask.dataframe as dd
import pandas as pd


def guess_type_list(value_list, quote_aware=True):
    """
    Guesses the most likely data type for each item in a list of values.

    Parameters:
        value_list (list): List of raw values (typically from one record or column)
        quote_aware (bool): If True, treat double-quoted values as strings regardless of content

    Returns:
        list: List of type guesses ('int', 'float', or 'str') for each item
    """
    guessed_types = []

    for val in value_list:
        if val is None:
            guessed_types.append('str')
            continue

        val_str = str(val).strip()

        if val_str == "":
            guessed_types.append('str')
            continue

        # --- QUOTE-AWARE SHORT-CIRCUIT ---
        if quote_aware and val_str.startswith('"') and val_str.endswith('"'):
            guessed_types.append('str')
            continue

        # --- Now safe to evaluate unquoted values only ---
        val_str_clean = val_str.strip()

        if '.' in val_str_clean:
            try:
                float(val_str_clean)
                guessed_types.append('float')
                continue
            except ValueError:
                pass

        try:
            int(val_str_clean)
            guessed_types.append('int')
        except ValueError:
            guessed_types.append('str')

    return guessed_types

def detect_sign_styles(value_list, quote_aware=True):
    """
    Detects the sign style (if any) used to represent negative values in a list of strings.

    Parameters:
        value_list (list): List of raw values (e.g., from a column or record)
        quote_aware (bool): If True, treat double-quoted values as non-numeric (returns 'str')

    Returns:
        list: One sign style per value:
              - 'leading'  → -123
              - 'trailing' → 123-
              - 'paren'    → (123)
              - 'none'     → positive or unsigned
              - 'str'      → quoted value or non-numeric-looking
    """
    sign_styles = []

    for val in value_list:
        if val is None:
            sign_styles.append('none')
            continue

        val_str = str(val).strip()

        if val_str == "":
            sign_styles.append('none')
            continue

        # --- QUOTE-AWARE SHORT-CIRCUIT ---
        if quote_aware and val_str.startswith('"') and val_str.endswith('"'):
            sign_styles.append('str')
            continue

        # --- Clean value ---
        val_str_clean = val_str.strip()

        if val_str_clean.startswith('(') and val_str_clean.endswith(')'):
            sign_styles.append('paren')
        elif val_str_clean.startswith('-'):
            sign_styles.append('leading')
        elif val_str_clean.endswith('-'):
            sign_styles.append('trailing')
        else:
            sign_styles.append('none')

    return sign_styles


def analyze_lengths_from_df(df: dd.DataFrame):
    """
    Analyzes the max field length and decimal precision (if applicable) for each column in a Dask DataFrame.

    Parameters:
        df (dask.DataFrame): Input DataFrame

    Returns:
        tuple: (length_list, precision_list), where:
            - length_list is the max number of characters per column
            - precision_list is the max number of digits after decimal (0 for non-numeric columns)
    """
    length_list = []
    precision_list = []

    # Sample up to 5000 rows for fast inspection
    try:
        sample_df = df.head(5000, compute=True)
    except Exception as e:
        raise ValueError(f"[analyze_lengths_from_df] Failed to sample DataFrame: {e}")

    for col in sample_df.columns:
        col_values = sample_df[col].dropna().astype(str)

        # --- Max length ---
        try:
            max_len = col_values.map(len).max()
        except Exception:
            max_len = 0
        length_list.append(max_len)

        # --- Decimal precision ---
        precision = 0
        try:
            # Coerce to floats where possible
            float_vals = pd.to_numeric(col_values, errors='coerce')
            valid_floats = float_vals.dropna()

            if not valid_floats.empty:
                def count_decimals(val):
                    try:
                        # Convert to string and split on decimal
                        if isinstance(val, float):
                            return len(str(val).split('.')[-1].rstrip('0'))
                        return 0
                    except Exception:
                        return 0

                precision = valid_floats.map(count_decimals).max()
        except Exception:
            precision = 0

        precision_list.append(precision)

    return length_list, precision_list


def init_length_tracker(num_fields):
    """
    Initializes a tracker dictionary for recording field lengths and decimal precision in streaming mode.

    Parameters:
        num_fields (int): Number of fields (columns) to track

    Returns:
        dict: A tracker with:
            - 'length_list': list of zeros for max string length
            - 'precision_list': list of zeros for decimal precision
    """
    return {
        'length_list': [0] * num_fields,
        'precision_list': [0] * num_fields
    }


def update_length_tracker(tracker, record):
    """
    Updates a streaming length tracker with a single record.

    Parameters:
        tracker (dict): The tracking dictionary (from init_length_tracker)
        record (list): A single record (list of values) to evaluate

    Returns:
        None – modifies tracker in place
    """
    for idx, val in enumerate(record):
        if val is None:
            continue

        val_str = str(val).strip()

        # --- Update max length ---
        current_len = len(val_str)
        if current_len > tracker['length_list'][idx]:
            tracker['length_list'][idx] = current_len

        # --- Update decimal precision if float-like ---
        if '.' in val_str:
            try:
                float_val = float(val_str)
                decimal_part = str(float_val).split('.')[-1].rstrip('0')
                precision = len(decimal_part)
                if precision > tracker['precision_list'][idx]:
                    tracker['precision_list'][idx] = precision
            except Exception:
                continue  # Ignore values that can't be parsed


def assign_default_flags(type_list,
                         sign_style_list=None,
                         length_list=None,
                         precision_list=None):
    """
    Assigns default normalization flags based on field types and optional metadata.

    Parameters:
        type_list (list[str]): Detected types per field ('int', 'float', 'str')
        sign_style_list (list[str], optional): Detected sign styles
        length_list (list[int], optional): Max field length (chars)
        precision_list (list[int], optional): Max decimal digits

    Returns:
        list[str]: Normalization flags per field
                   - 'strict'    → full normalization (numeric alignment, signs, etc.)
                   - 'pad_only' → just pad strings to field length
                   - 'skip'     → no normalization
    """
    flags = []

    for idx, field_type in enumerate(type_list):
        if field_type in ('int', 'float'):
            # Optional: check for missing precision/sign style and fallback
            sign_style = sign_style_list[idx] if sign_style_list else 'leading'
            precision = precision_list[idx] if precision_list else 0

            if field_type == 'float' and precision == 0:
                # probably misclassified or mixed data, skip or pad only
                flags.append('pad_only')
            else:
                flags.append('strict')

        elif field_type == 'str':
            flags.append('pad_only')
        else:
            flags.append('skip')

    return flags


def generate_linkage_config(df_or_sample, field_names, mode='batch', quote_aware=True):
    """
    Generates a complete normalization linkage config based on field data.

    Parameters:
        df_or_sample (DataFrame or list of records): A full Dask dataframe or a sampled list of records
        field_names (list): List of field names (ordered)
        mode (str): 'batch' or 'stream'
        quote_aware (bool): Treat quoted values as strings if True

    Returns:
        dict: Full linkage config ready for normalization
    """
    num_fields = len(field_names)
    col_list = list(range(num_fields))
    type_list = []
    length_list = []
    precision_list = []
    sign_style_list = []

    print(f"[DEBUG] linkage_helpers.generate_linkage_config called with mode = {mode}")

    if mode.strip().lower() == 'batch':

        # --- Assume df_or_sample is a Dask DataFrame ---
        df = df_or_sample

        # --- Analyze length and precision ---
        length_list, precision_list = analyze_lengths_from_df(df)

        # --- Guess type for each column ---
        try:
            sample_df = df.head(5000, compute=True)
        except Exception as e:
            raise ValueError(f"[generate_linkage_config] Failed to sample DataFrame: {e}")

        for col in sample_df.columns:
            type_guess = guess_type_list(sample_df[col].tolist(), quote_aware=quote_aware)
            # Most common guess across sample values
            if type_guess:
                most_common = max(set(type_guess), key=type_guess.count)
            else:
                most_common = 'str'
            type_list.append(most_common)

            sign_styles = detect_sign_styles(sample_df[col].tolist(), quote_aware=quote_aware)
            if sign_styles:
                most_common_sign = max(set(sign_styles), key=sign_styles.count)
            else:
                most_common_sign = 'none'
            sign_style_list.append(most_common_sign)

    elif mode.strip().lower() == 'stream':
        # --- Assume df_or_sample is a list of records ---
        tracker = init_length_tracker(num_fields)
        type_votes = [[] for _ in range(num_fields)]
        sign_votes = [[] for _ in range(num_fields)]

        for record in df_or_sample:
            update_length_tracker(tracker, record)

            type_guess = guess_type_list(record, quote_aware=quote_aware)
            sign_guess = detect_sign_styles(record, quote_aware=quote_aware)

            for i in range(num_fields):
                type_votes[i].append(type_guess[i])
                sign_votes[i].append(sign_guess[i])

        length_list = tracker["length_list"]
        precision_list = tracker["precision_list"]

        for i in range(num_fields):
            most_common_type = max(set(type_votes[i]), key=type_votes[i].count)
            most_common_sign = max(set(sign_votes[i]), key=sign_votes[i].count)
            type_list.append(most_common_type)
            sign_style_list.append(most_common_sign)

    else:
        print(f"[DEBUG] Unhandled mode value: {repr(mode)}")
        raise ValueError(f"[generate_linkage_config] Invalid mode: {mode}")


    # --- Assign default flags ---
    normalize_flags = assign_default_flags(
        type_list,
        sign_style_list=sign_style_list,
        length_list=length_list,
        precision_list=precision_list
    )

    return {
        "col_list": col_list,
        "type_list": type_list,
        "length_list": length_list,
        "precision_list": precision_list,
        "sign_style_list": sign_style_list,
        "normalize_flags": normalize_flags,
        "field_names": field_names,
        "mode": mode
    }







