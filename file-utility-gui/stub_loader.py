# stub_loader.py
import csv, io, logging
from typing import Dict, List, Tuple, Optional

def _sniff_delimiter(sample: str) -> str:
    if ';' in sample and ',' in sample:
        return ';' if sample.count(';') >= sample.count(',') else ','
    for cand in (';', ',', '\t', '|'):
        if cand in sample:
            return cand
    return ','

def _names_from_header(header: List[str], fields: List[int] | List[str]) -> List[str]:
    out = []
    for f in fields:
        if isinstance(f, int):
            if f < 0 or f >= len(header):
                raise IndexError(f"Field index {f} out of range (header has {len(header)} cols).")
            out.append(header[f])
        else:
            out.append(f)
    return out

def _composite(parts: List[str]) -> str:
    return '||'.join(parts)

def load_stub_lookup_table_v2(
    stub_file_path: str,
    key_fields: List[int] | List[str],
    stub_fields: List[int] | List[str],
    *,
    logger: Optional[logging.Logger] = None,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    max_log_rows: int = 20,
) -> Tuple[Dict[str, List[dict]], List[str], List[str], str, str]:
    """
    Load a stub lookup table with:
      - NO numeric casting (preserve leading zeros)
      - explicit delimiter/encoding handling
      - targeted row-level debug logging

    Returns: (lookup, key_field_names, stub_field_names, used_encoding, used_delim)
    """
    log = logger or logging.getLogger("stub_loader")
    log.setLevel(logging.DEBUG)

    # --- read raw bytes (for encoding + delimiter sniffing) ---
    try:
        with open(stub_file_path, "rb") as fb:
            raw = fb.read(65536)  # 64KB
    except Exception as e:
        log.error(f"[STUB] Failed reading {stub_file_path}: {e}")
        raise

    # --- encoding ---
    used_encoding = encoding or "utf-8"
    sample_text = None
    for enc_try in ([used_encoding] if encoding else ["utf-8", "cp1252"]):
        try:
            sample_text = raw.decode(enc_try, errors="strict")
            used_encoding = enc_try
            break
        except Exception:
            pass
    if sample_text is None:
        sample_text = raw.decode("utf-8", errors="replace")
        used_encoding = encoding or "utf-8"
        log.warning(f"[STUB] Permissive decode in use; encoding uncertain -> {used_encoding}")

    # --- delimiter ---
    used_delim = delimiter or _sniff_delimiter(sample_text)
    log.info(f"[STUB] Using encoding={used_encoding}, delimiter={repr(used_delim)} for {stub_file_path}")

    # --- parse header once with csv.reader ---
    text_stream = io.StringIO(raw.decode(used_encoding, errors="replace"))
    reader = csv.reader(text_stream, delimiter=used_delim)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("Stub file is empty.")
    except Exception as e:
        log.error(f"[STUB] Failed reading header: {e}")
        raise

    key_field_names  = _names_from_header(header, key_fields)
    stub_field_names = _names_from_header(header, stub_fields)

    # --- switch to DictReader ---
    text_stream.seek(0)
    dict_reader = csv.DictReader(text_stream, delimiter=used_delim)
    if dict_reader.fieldnames is None:
        raise ValueError("Failed to read header from stub file")

    lookup: Dict[str, List[dict]] = {}
    total_rows = 0
    missing_cols = 0
    logged_rows = 0

    for line_num, row in enumerate(dict_reader, start=2):
        total_rows += 1

        if any((fn not in row) for fn in key_field_names + stub_field_names):
            missing_cols += 1
            if logged_rows < max_log_rows:
                log.warning(f"[STUB] L{line_num}: Missing required field(s); have={list(row.keys())}")
                logged_rows += 1
            continue

        # build key (NO casting; preserve leading zeros)
        key_parts = [("" if row[k] is None else str(row[k]).strip()) for k in key_field_names]
        comp_key  = _composite(key_parts)

        payload = {f: ("" if row[f] is None else str(row[f]).strip()) for f in stub_field_names}

        if logged_rows < max_log_rows:
            log.debug(f"[STUB] L{line_num}: key_parts={key_parts} key={comp_key} payload={payload}")
            logged_rows += 1

        lookup.setdefault(comp_key, []).append(payload)

    log.info(f"[STUB] Loaded rows={total_rows}, missing_cols={missing_cols}, distinct_keys={len(lookup)}")
    if total_rows == 0:
        log.warning("[STUB] No data rows after header.")
    if len(lookup) == 0:
        log.warning("[STUB] Empty lookup built. Verify delimiter, header names, and field mapping.")

    return lookup, key_field_names, stub_field_names, used_encoding, used_delim
