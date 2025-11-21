# sorter_7x.py
# Sorts a DataFrame by one or more fields and writes to CSV (supports Dask DataFrame)

import dask.dataframe as dd
import csv
from io import StringIO
from utils_11x_gui import safe_open


def sort_records(df_or_path, output_file, sort_fields, delimiter=",", quote_preserve=True, fallback=True,
                 logger=None):
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
    import logging
    if logger is None:
        import logging
        logging.basicConfig(level=logging.DEBUG)  # ⬅️ Raise default level
        logger = logging.getLogger("sorter_fallback")
        logger.setLevel(logging.DEBUG)  # ⬅️ Explicitly allow debug logs
    else:
        logger.setLevel(logging.DEBUG)  # ⬅️ Force even if passed in

    reject_lines = []  # <-- Needed in both quote_preserve and non-quote_preserve paths

    logger.info( f"[7x_Sorter] after import 22 - quote_preserve received: {quote_preserve}" )

    # ---- MODE 1: QUOTE PRESERVE (NO STRIP) ----
    if quote_preserve:
        logger.info("[7x_Sorter] Preserving original quotes using raw line sort.")

        if not isinstance(df_or_path, str):
            logger.error("[7x_Sorter] ERROR: Expected file path in quote_preserve mode, got DataFrame instead.")
            return None
        else:
            logger.info(f"[7x_Sorter] Received input file path: {df_or_path}")

        # Open file safely with fallback encoding
        _, f_enc, _ = safe_open(df_or_path, mode="r", flexible=True)

        with open(df_or_path, mode="r", encoding=f_enc, newline="") as f:
            header_line = f.readline()
            header_fields = header_line.strip().split(delimiter)
            data_lines = f.readlines()
            reject_lines = []

        # Map sort_fields to column indices
        sort_indices = []
        for field in sort_fields:
            if field not in header_fields:
                raise ValueError(f"[7x_Sorter] Field not found in header: {field}")
            sort_indices.append(header_fields.index(field))

        msg = "\n=== SORT ORDER CONFIRMATION ==="
        print(msg)
        logger.info(msg)

        for i, field in enumerate(sort_fields, 1):
            msg = f"[{i}] {field}"
            print(msg)
            logger.info(msg)

        logger.info("===============================\n")

        def extract_sort_keys(line):
            if not line.strip():
                return tuple()  # Skip blank lines
            try:
                reader = csv.reader(StringIO(line), delimiter=delimiter, quotechar='"')
                fields = next(reader, None)
                # logger.debug(f"[QUOTE-PRESERVE] Parsed fields: {fields}")
                if not fields or all(not str(f).strip() for f in fields):
                    # logger.debug("[QUOTE-PRESERVE] Skipping parsed empty or whitespace-only row.")
                    return tuple()

            except Exception as e:
                logger.warning(f"[WARN] Failed to parse line: {line.strip()[:80]} -- {e}")
                reject_lines.append(line)
                return tuple()

            keys = []
            for i in sort_indices:
                if i >= len(fields):
                    logger.error(f"[ERROR] Sort field index {i} out of range for line: {line.strip()[:100]}")
                    reject_lines.append(line)
                    return tuple()
                val = fields[i].strip().strip('"')
                try:
                    keys.append(int(val))
                except ValueError:
                    try:
                        keys.append(float(val))
                    except ValueError:
                        keys.append(val)
            return tuple(keys)

        # Perform sort
        sorted_lines = sorted(data_lines, key=extract_sort_keys)

        # Write output safely with cleaned newlines
        with open(output_file, mode="w", encoding="utf-8", newline="") as f:
            # Write header EXACTLY as in the input
            f.write(header_line if header_line.endswith("\n") else header_line + "\n")

            # Write each row EXACTLY as in the input (preserve quoting and delimiters)
            for line in sorted_lines:
                f.write(line if line.endswith("\n") else line + "\n")

        logger.info(f"[7x_Sorter] Output written to: {output_file}")

        # Optionally write rejected lines
        if reject_lines:
            reject_path = output_file.replace("._RESULTS", "._REJECTS")
            with open(reject_path, mode="w", encoding="utf-8", newline="") as rej:
                rej.writelines(reject_lines)
            print(f"[WARN] {len(reject_lines)} malformed line(s) written to: {reject_path}")

        print(f"[DEBUG] Returning sorted output file: {output_file}")
        return output_file

    # ---- MODE 2: STRIP QUOTES USING DASK ----
    # logger.info("[7x_Sorter] - quote_preserve received: False")

    reject_lines = []  # Ensure reject_lines is always defined

    # Load with Dask for parallel sort; dtype=str to ensure consistent column handling
    if isinstance(df_or_path, dd.DataFrame):
        df = df_or_path
        logger.info("[7x_Sorter] Using provided Dask DataFrame (normalized or preloaded).")
        f_enc = "utf-8"  # Assume default if already loaded
    else:
        _, f_enc, _ = safe_open(df_or_path, mode="r", flexible=fallback)
        logger.info(f"[7x_Sorter] Detected encoding: {f_enc}")
        df = dd.read_csv(
            df_or_path,
            sep=delimiter,
            encoding=f_enc,
            assume_missing=True,
            dtype=str
        )

    msg = "\n=== SORT ORDER CONFIRMATION ==="
    print(msg)
    logger.info(msg)

    for i, field in enumerate(sort_fields, 1):
        msg = f"[{i}] {field}"
        print(msg)
        logger.info(msg)

    msg = "===============================\n"
    print(msg)
    logger.info(msg)

    sorted_df = df.sort_values(by=sort_fields)

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

    logger.info(f"[7x_Sorter] Output written to: {output_file}")
    import os
    print(f"[DEBUG] Checking if output file was created: {output_file}")
    print(f"[DEBUG] os.path.exists: {os.path.exists(output_file)}")

    # Optionally write rejected lines
    if reject_lines:
        reject_path = output_file.replace("._RESULTS", "._REJECTS")
        with open(reject_path, mode="w", encoding="utf-8", newline="") as rej:
            rej.writelines(reject_lines)
        print(f"[WARN] {len(reject_lines)} malformed line(s) written to: {reject_path}")

    return output_file



