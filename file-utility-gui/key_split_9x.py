"""
Option 9 — Split by Key (Match / No Match)

Public entry points:
  • prepare_preview(config, logger) -> preview_payload
  • run(config, logger, shared_data) -> result_payload

Behavior:
  - Build a KEY set from the key file using composite key mapping (names resolved to indices by the UI).
  - Stream MAIN once; write each row to either MATCHED or NONMATCHED output based on presence in KEY.
  - No column appends; rows pass through unchanged.

Config keys expected:
  main_file, main_delimiter, main_has_header
  key_file,  key_delimiter,  key_has_header
  match_fields_main: [int]
  match_fields_key:  [int]
  case_insensitive: bool (default True)
  strip_whitespace: bool (default True)
  auto_zero_pad: bool (default True)
  zero_pad_widths: [int] | {pos:int} (optional explicit widths per key position)
  matched_output_file: str (default <main>_MATCHED.csv)
  nonmatched_output_file: str (default <main>_NONMATCHED.csv)
  write_header: bool (default = main_has_header)
"""
from __future__ import annotations

from typing import Dict, Any, List, Tuple
from pathlib import Path
import csv
import unicodedata

# ------------------------------
# Normalization helpers
# ------------------------------
_EMPTY_CHARS = dict.fromkeys(map(ord, "\ufeff\u00a0\u200b\u200c\u200d"), None)  # BOM, NBSP, ZW*, etc.

def _norm_cell(s: str, *, strip_ws: bool = True, case_insensitive: bool = True) -> str:
    if s is None:
        return ""
    s = s.translate(_EMPTY_CHARS)
    s = unicodedata.normalize("NFKC", s)
    if strip_ws:
        s = s.strip()
    if case_insensitive:
        s = s.lower()
    return s


def _probe_encoding(path: Path) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            f.read(2048)
        return "utf-8"
    except Exception:
        return "cp1252"


def _zpad(s: str, width: int | None) -> str:
    if not width or width <= 0:
        return s
    return s.zfill(width) if s.isdigit() else s


def _build_key(parts: List[str], fields: List[int], pad_widths, *, strip_ws: bool, case_insensitive: bool) -> Tuple[str, ...]:
    out: List[str] = []
    for pos, idx in enumerate(fields):
        v = parts[idx] if 0 <= idx < len(parts) else ""
        v = _norm_cell(v, strip_ws=strip_ws, case_insensitive=case_insensitive)
        w = 0
        if isinstance(pad_widths, list):
            w = pad_widths[pos] if pos < len(pad_widths) else 0
        elif isinstance(pad_widths, dict):
            w = pad_widths.get(pos, 0)
        out.append(_zpad(v, w))
    return tuple(out)


# ------------------------------
# Preview
# ------------------------------

def prepare_preview(config: Dict[str, Any], logger) -> Dict[str, Any]:
    logger.info("key_split_9x VERSION: 2025-08-28-a")

    main_file = Path(config.get("main_file", ""))
    key_file  = Path(config.get("key_file", ""))
    main_delim = config.get("main_delimiter", ",")
    key_delim  = config.get("key_delimiter", ",")
    main_has_header = bool(config.get("main_has_header", True))
    key_has_header  = bool(config.get("key_has_header", True))

    if not main_file.exists():
        raise FileNotFoundError(f"Main file not found: {main_file}")
    if not key_file.exists():
        raise FileNotFoundError(f"Key file not found: {key_file}")

    enc_main = _probe_encoding(main_file)
    enc_key  = _probe_encoding(key_file)

    # Quick header read
    main_header: List[str] = []
    key_header: List[str] = []
    if main_has_header:
        with open(main_file, "r", encoding=enc_main, newline="") as f:
            main_header = next(csv.reader(f, delimiter=main_delim), [])
    if key_has_header:
        with open(key_file, "r", encoding=enc_key, newline="") as f:
            key_header = next(csv.reader(f, delimiter=key_delim), [])

    return {
        "main_file": str(main_file),
        "key_file": str(key_file),
        "main_delimiter": main_delim,
        "key_delimiter": key_delim,
        "main_has_header": main_has_header,
        "key_has_header": key_has_header,
        "main_header": main_header,
        "key_header": key_header,
        "enc_main": enc_main,
        "enc_key": enc_key,
    }


# ------------------------------
# Run
# ------------------------------

def run(config: Dict[str, Any], logger, shared_data: Dict[str, Any]) -> Dict[str, Any]:
    main_file = Path(config.get("main_file", ""))
    key_file  = Path(config.get("key_file", ""))
    main_delim = config.get("main_delimiter", ",")
    key_delim  = config.get("key_delimiter", ",")
    main_has_header = bool(config.get("main_has_header", True))
    key_has_header  = bool(config.get("key_has_header", True))

    case_insensitive = bool(config.get("case_insensitive", True))
    strip_ws = bool(config.get("strip_whitespace", True))
    auto_zero_pad = bool(config.get("auto_zero_pad", True))
    pad_widths = config.get("zero_pad_widths")  # optional

    main_fields: List[int] = list(config.get("match_fields_main") or [])
    key_fields:  List[int] = list(config.get("match_fields_key") or [])
    if not main_fields or not key_fields or len(main_fields) != len(key_fields):
        raise ValueError("match_fields_main and match_fields_key must be non-empty and of equal length.")

    # Output paths
    default_match = main_file.with_name(main_file.stem + "_MATCHED.csv")
    default_non   = main_file.with_name(main_file.stem + "_NONMATCHED.csv")
    out_match = Path(config.get("matched_output_file") or default_match)
    out_non   = Path(config.get("nonmatched_output_file") or default_non)
    counts_path = main_file.with_name(main_file.stem + "_SPLIT_COUNTS.txt")
    write_header = bool(config.get("write_header", config.get("main_has_header", True)))

    # Build KEY set and learn zero-pad widths if needed
    enc_key = _probe_encoding(key_file)
    if not pad_widths and auto_zero_pad:
        learned = [0] * len(key_fields)
        with open(key_file, "r", encoding=enc_key, newline="") as fk:
            r = csv.reader(fk, delimiter=key_delim)
            if key_has_header:
                next(r, None)
            for row in r:
                if not row:
                    continue
                for pos, idx in enumerate(key_fields):
                    v = _norm_cell(row[idx] if 0 <= idx < len(row) else "",
                                   strip_ws=strip_ws, case_insensitive=case_insensitive)
                    if v.isdigit() and len(v) > learned[pos]:
                        learned[pos] = len(v)
        pad_widths = learned
        logger.info(f"[9x] Auto zero-pad widths (from KEY): {pad_widths}")

    key_set: set[Tuple[str, ...]] = set()
    with open(key_file, "r", encoding=enc_key, newline="") as fk:
        r = csv.reader(fk, delimiter=key_delim)
        if key_has_header:
            next(r, None)
        for row in r:
            if not row:
                continue
            k = _build_key(row, key_fields, pad_widths, strip_ws=strip_ws, case_insensitive=case_insensitive)
            key_set.add(k)
    logger.info(f"[9x] Key set size: {len(key_set)}")

    # Stream MAIN and split
    enc_main = _probe_encoding(main_file)
    out_match.parent.mkdir(parents=True, exist_ok=True)
    out_non.parent.mkdir(parents=True, exist_ok=True)

    rows_read = 0
    rows_match = 0
    rows_non   = 0

    # IMPORTANT: use parentheses for multi-context 'with' in Python
    with (
        open(main_file, "r", encoding=enc_main, newline="") as fin,
        open(out_match, "w", encoding="utf-8", newline="") as fmatch,
        open(out_non,   "w", encoding="utf-8", newline="") as fnon
    ):
        rin = csv.reader(fin, delimiter=main_delim)
        wmatch = csv.writer(fmatch, delimiter=main_delim)
        wnon   = csv.writer(fnon,   delimiter=main_delim)

        header: List[str] = []
        if main_has_header:
            header = next(rin, [])
            if write_header and header:
                wmatch.writerow(header)
                wnon.writerow(header)

        for row in rin:
            # robust blank-row guard (handles "", " , , ", etc.)
            if not row or not any((cell or "").strip() for cell in row):
                continue
            rows_read += 1
            k = _build_key(row, main_fields, pad_widths, strip_ws=strip_ws, case_insensitive=case_insensitive)
            if k in key_set:
                wmatch.writerow(row)
                rows_match += 1
            else:
                wnon.writerow(row)
                rows_non += 1

    result = {
        "main_file": str(main_file),
        "key_file": str(key_file),
        "matched_output_file": str(out_match),
        "nonmatched_output_file": str(out_non),
        "counts_report_path": str(counts_path),
        "rows_read_main": rows_read,
        "rows_matched": rows_match,
        "rows_nonmatched": rows_non,
        "main_delimiter": main_delim,
        "key_delimiter": key_delim,
        "main_has_header": main_has_header,
        "key_has_header": key_has_header,
        "match_fields_main": main_fields,
        "match_fields_key": key_fields,
        "case_insensitive": case_insensitive,
        "strip_whitespace": strip_ws,
        "auto_zero_pad": auto_zero_pad,
        "zero_pad_widths": pad_widths or [],
    }

    try:
        counts_path.write_text(_format_report(result), encoding="utf-8")
    except Exception:
        pass

    logger.info("\n" + _format_report(result))
    # match file is the "primary" artifact for downstream steps
    shared_data["output_file"] = str(out_match)
    return result


# ------------------------------
# Reporting
# ------------------------------

def _format_report(r: Dict[str, Any]) -> str:
    pad = " " * 2
    lines: List[str] = []
    lines.append("OPTION 9 — SPLIT BY KEY (MATCH / NO MATCH) — CONTROL REPORT")
    lines.append("")
    lines.append(f"Main File      : {r.get('main_file','')}")
    lines.append(f"Key File       : {r.get('key_file','')}")
    lines.append(f"Matched File   : {r.get('matched_output_file','')}")
    lines.append(f"Non-Matched    : {r.get('nonmatched_output_file','')}")
    lines.append(f"Counts Report  : {r.get('counts_report_path','')}")
    lines.append("")
    lines.append(f"Main delimiter : {r.get('main_delimiter',',')}")
    lines.append(f"Key delimiter  : {r.get('key_delimiter',',')}")
    lines.append("")
    mf = r.get("match_fields_main") or []
    kf = r.get("match_fields_key") or []
    lines.append("MATCH FIELDS:")
    for i, (a, b) in enumerate(zip(mf, kf), start=1):
        lines.append(f"{pad}{i}) MAIN[{a}] ↔ KEY[{b}]")
    lines.append("")
    lines.append("TOTALS:")
    lines.append(f"{pad}Rows read (MAIN)      : {r.get('rows_read_main',0)}")
    lines.append(f"{pad}Rows matched          : {r.get('rows_matched',0)}")
    lines.append(f"{pad}Rows non-matched      : {r.get('rows_nonmatched',0)}")
    return "\n".join(lines) + "\n"
