# deduper_8x.py
# Deduplication utility module supporting two modes:
# 1. 'dedupe': Keep first occurrence, remove duplicates
# 2. 'keep_duplicates_only': Keep only duplicate records

import dask.dataframe as dd
import logging


logger = logging.getLogger("deduper_8x")
if logger is None:
    logger = logging.getLogger()

def normalize_record(row, shared_data):
    """
    Wrapper to call normalize_streaming_record with config stored in shared_data.
    """
    from normalizer_core import normalize_streaming_record
    config = shared_data.get("normalize_config", {})
    return normalize_streaming_record(row, config)


def apply_normalization_partition(partition, func):
    return partition.apply(func, axis=1)

def dedupe_records(df, dedupe_fields, mode="dedupe", logger=None, normalize_flag=False, shared_data=None):
    """
    Deduplicates a Dask DataFrame based on specified fields.

    Parameters:
        df (dask.DataFrame): Input DataFrame
        dedupe_fields (list|str): Columns to evaluate duplicates on
        mode (str): 'dedupe' or 'keep_duplicates_only'
        normalize_flag (bool): Apply normalize_record() (whole-row) before dedupe
        shared_data (dict): may contain 'output_header' and normalize config

    Returns:
        dask.DataFrame or pandas.DataFrame (for 'dedupe' mode): Processed result
    """
    # Logger fallback
    if logger is None:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("deduper_8x")

    try:
        logger.info("[8x_Deduper] dedupe_records() called")
        logger.info(f"[8x_Deduper] Dedupe mode selected: {mode}")

        # Canonicalize dedupe_fields into a list of names
        if isinstance(dedupe_fields, str):
            raw = dedupe_fields.strip()
            sep = ';' if ';' in raw else (',' if ',' in raw else ('\t' if '\t' in raw else None))
            dedupe_fields = [c.strip() for c in (raw.split(sep) if sep else [raw]) if c.strip()]
        elif (
            isinstance(dedupe_fields, (list, tuple))
            and len(dedupe_fields) == 1
            and isinstance(dedupe_fields[0], str)
            and any(sep in dedupe_fields[0] for sep in (';', ',', '\t'))
        ):
            raw = dedupe_fields[0]
            sep = ';' if ';' in raw else (',' if ',' in raw else '\t')
            dedupe_fields = [c.strip() for c in raw.split(sep) if c.strip()]

        # Must be a Dask DataFrame
        import dask.dataframe as dd
        if not isinstance(df, dd.DataFrame):
            raise TypeError("dedupe_records() requires a Dask DataFrame as input.")

        # === GUARD A: header alignment (only if exact width matches) ===
        out_hdr = (shared_data or {}).get("output_header")
        try:
            if out_hdr and len(out_hdr) == len(df.columns):
                df.columns = [str(c) for c in out_hdr]
            else:
                logger.info(
                    "[8x_Deduper] Skipping header alignment: df has %d cols, output_header has %s.",
                    len(df.columns), (len(out_hdr) if out_hdr else "None"),
                )
        except Exception as e:
            logger.warning(f"[8x_Deduper] Header alignment skipped due to error: {e}")

        # === GUARD 0: require fields ===
        if not dedupe_fields:
            raise ValueError("No dedupe fields provided.")

        # Remove accidental duplicates while preserving order
        _seen = set()
        dedupe_fields = [c for c in dedupe_fields if not (c in _seen or _seen.add(c))]

        logger.info(f"[8x_Deduper] Fields: {dedupe_fields}")

        # === GUARD B: ensure selected columns exist ===
        missing = [c for c in dedupe_fields if c not in df.columns]
        if missing:
            raise ValueError(
                f"Selected dedupe columns not found in file: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # =========================
        # Normalization phase (whole-row, optional)
        # =========================
        df_normalized = df
        if normalize_flag:
            logger.info("[8x_Deduper] Applying normalize_record() to all rows")
            from functools import partial
            from normalizer_router import normalize_record as normalize_record_router

            # Copy & drop unpicklables for Dask graph safety
            safe_data = dict(shared_data or {})
            for key in list(safe_data.keys()):
                val = safe_data[key]
                if "DataFrame" in str(type(val)) or "tkinter" in str(type(val)) or callable(val):
                    del safe_data[key]

            def apply_normalization_partition(partition, func):
                return partition.apply(func, axis=1)

            normalize_with_data = partial(normalize_record_router, shared_data=safe_data)
            df_normalized = df.map_partitions(
                apply_normalization_partition, normalize_with_data, meta=df._meta
            )

        # === Key normalization ALWAYS (stringify/strip/lower) ===
        for col in dedupe_fields:
            df_normalized[col] = df_normalized[col].astype(str).str.strip().str.lower()

        # === Dedupe logic ===
        if mode == "dedupe":
            # Build composite key WITHOUT DataFrame.apply (avoid shape/key errors)
            key_series = None
            for i, c in enumerate(dedupe_fields):
                s = df_normalized[c].fillna("").astype(str)  # avoid "nan"
                key_series = s if i == 0 else (key_series + "||" + s)

            df_normalized["_dedupe_key"] = key_series

            # Diagnostic peek (light materialization)
            try:
                logger.info("[8x_Deduper] Preview after _dedupe_key assignment:")
                logger.info("Column count: %d", len(df_normalized.columns))
                logger.info("\n%s", df_normalized.head(5))
            except Exception as diag_err:
                logger.warning("[8x_Deduper] Could not preview after key assignment: %s", diag_err)

            # Pull to pandas and drop duplicates on composite key
            pdf = df_normalized.compute()
            pre = len(pdf)

            # (Optional) light re-normalization if upstream changed; key already exists
            for col in dedupe_fields:
                pdf[col] = pdf[col].astype(str).str.strip().str.lower()

            pdf = pdf.drop_duplicates(subset="_dedupe_key", keep="first")
            post = len(pdf)
            logger.info("\n=== DEDUPE SUMMARY ===")
            logger.info(f"[INFO] Total rows before dedupe : {pre}")
            logger.info(f"[INFO] Total rows after dedupe  : {post}")
            logger.info(f"[INFO] Duplicate rows removed   : {pre - post}")
            pdf = pdf.drop(columns=["_dedupe_key"], errors="ignore")

            # Optionally align final header to composite (if provided and matches)
            try:
                if out_hdr and len(out_hdr) == len(pdf.columns):
                    pdf.columns = [str(c) for c in out_hdr]
            except Exception as hdr_err:
                logger.warning(f"[8x_Deduper] Could not reset columns to output_header: {hdr_err}")

            return pdf

        elif mode == "keep_duplicates_only":
            logger.info("[8x_Deduper] KEEP DUPLICATES logic triggered.")
            # Dask-compatible: name the size via to_frame, then reset_index
            group_counts = (
                df_normalized.groupby(dedupe_fields)
                .size()
                .to_frame("_count")
                .reset_index()
            )
            dup_keys = group_counts[group_counts["_count"] > 1][dedupe_fields]
            result = df_normalized.merge(dup_keys, on=dedupe_fields, how="inner")
            result = result.drop(columns=["_dedupe_key"], errors="ignore")
            return result

        else:
            raise ValueError(f"Unsupported dedupe mode: {mode}")

    except Exception as e:
        logger.error(f"[8x_Deduper] ERROR during deduplication: {e}")
        raise
