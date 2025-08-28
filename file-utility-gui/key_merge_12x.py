"""
Option 12 — Merge by Key (inner / anti)

Public entry points:
  • prepare_preview(config, logger) -> preview_payload
  • run(config, logger, shared_data) -> result_payload

This module filters the MAIN file by looking up composite keys in a KEY file.
No columns are appended; rows are either kept (inner join) or excluded (anti-join).

Config keys expected (most are optional; sensible defaults provided):
  - main_file: str
  - main_delimiter: str (default ",")
  - main_has_header: bool (default True)
  - key_file: str
  - key_delimiter: str (default ",")
  - key_has_header: bool (default True)
  - match_fields_main: List[int]  # indexes into MAIN rows
  - match_fields_key:  List[int]  # indexes into KEY rows (same length & order)
  - join_mode: str  # "inner" (keep matches) or "anti" (keep non-matches); default "inner"
  - case_insensitive: bool (default True)
  - strip_whitespace: bool (default True)
  - zero_pad_widths: List[int] | Dict[int,int]  # per-field pad widths (optional)
  - output_file: str (default = main_file with "_RESULTS.csv")
  - write_header: bool (default = main_has_header)

This is a clean, streaming implementation. Encoding is probed (utf-8 → cp1252 fallback).
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


def _open_text(path: Path, mode: str = "r", encoding: str | None = None):
    enc = encoding or _probe_encoding(path)
    return open(path, mode, encoding=enc, newline=""), enc


def _zpad(s: str, width: int | None) -> str:
    if not width or width <= 0:
        return s
    return s.zfill(width) if s.isdigit() else s


def _build_key(parts: List[str], fields: List[int], pad_widths: List[int] | Dict[int, int] | None,
               *, strip_ws: bool, case_insensitive: bool) -> Tuple[str, ...]:
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
    logger.info("key_merge_12x VERSION: 2025-08-28-a")

    main_file = Path(config.get("main_file", ""))
    key_file = Path(config.get("key_file", ""))
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

    # Quick header read for counts
    with open(main_file, "r", encoding=enc_main, newline="") as f:
        r = csv.reader(f, delimiter=main_delim)
        main_header = next(r, []) if main_has_header else []
    with open(key_file, "r", encoding=enc_key, newline="") as f:
        r = csv.reader(f, delimiter=key_delim)
        key_header = next(r, []) if key_has_header else []

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
    key_file = Path(config.get("key_file", ""))
    main_delim = config.get("main_delimiter", ",")
    key_delim  = config.get("key_delimiter", ",")
    main_has_header = bool(config.get("main_has_header", True))
    key_has_header  = bool(config.get("key_has_header", True))
    join_mode = (config.get("join_mode") or "inner").lower()
    if join_mode not in ("inner", "anti"):
        logger.warning(f"[12x] Unknown join_mode={join_mode!r}; defaulting to 'inner'")
        join_mode = "inner"

    case_insensitive = bool(config.get("case_insensitive", True))
    strip_ws = bool(config.get("strip_whitespace", True))
    pad_widths = config.get("zero_pad_widths")  # list or dict (per-key position)

    main_fields: List[int] = list(config.get("match_fields_main") or [])
    key_fields:  List[int] = list(config.get("match_fields_key") or [])
    auto_zero_pad = bool(config.get("auto_zero_pad", True))

    if not main_fields or not key_fields or len(main_fields) != len(key_fields):
        raise ValueError("match_fields_main and match_fields_key must be non-empty and of equal length.")

    # Output paths
    output_path = Path(config.get("output_file") or main_file.with_name(main_file.stem + "_RESULTS.csv"))
    counts_path = output_path.with_name(output_path.stem + "_COUNTS.txt")
    write_header = bool(config.get("write_header", config.get("main_has_header", True)))

    enc_key = _probe_encoding(key_file)

    # Learn digit widths from KEY (per key-field position)
    if not pad_widths and auto_zero_pad:
        learned = [0] * len(key_fields)
        with open(key_file, "r", encoding=enc_key, newline="") as fk:
            reader = csv.reader(fk, delimiter=key_delim)
            if key_has_header:
                next(reader, None)
            for row in reader:
                if not row:
                    continue
                for pos, idx in enumerate(key_fields):
                    v = _norm_cell(row[idx] if 0 <= idx < len(row) else "",
                                   strip_ws=strip_ws, case_insensitive=case_insensitive)
                    if v.isdigit():
                        if len(v) > learned[pos]:
                            learned[pos] = len(v)
        pad_widths = learned
        logger.info(f"[12x] Auto zero-pad widths (from KEY): {pad_widths}")

    # Build KEY set (normalized composite keys from key file)
    key_set: set[tuple[str, ...]] = set()
    with open(key_file, "r", encoding=enc_key, newline="") as f_key:
        reader = csv.reader(f_key, delimiter=key_delim)
        if key_has_header:
            next(reader, None)
        for row in reader:
            if not row:
                continue
            row_key = _build_key(row, key_fields, pad_widths,
                                 strip_ws=strip_ws, case_insensitive=case_insensitive)
            key_set.add(row_key)

    logger.info(f"[12x] Key set size: {len(key_set)}")
    # Stream MAIN and write filter result
    records_read = 0
    records_written = 0
    f_in, enc_main = _open_text(main_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with f_in, open(output_path, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.reader(f_in, delimiter=main_delim)
        writer = csv.writer(f_out, delimiter=main_delim)

        if main_has_header:
            header = next(reader, [])
            if write_header and header:
                writer.writerow(header)

        for row in reader:
            if not row:
                continue
            records_read += 1
            key_main = _build_key(row, main_fields, pad_widths, strip_ws=strip_ws, case_insensitive=case_insensitive)
            keep = (key_main in key_set) if join_mode == "inner" else (key_main not in key_set)
            if keep:
                writer.writerow(row)
                records_written += 1

    # Report
    result = {
        "main_file": str(main_file),
        "key_file": str(key_file),
        "output_file": str(output_path),
        "counts_report_path": str(counts_path),
        "join_mode": join_mode,
        "records_read": records_read,
        "records_written": records_written,
        "main_delimiter": main_delim,
        "key_delimiter": key_delim,
        "main_has_header": main_has_header,
        "key_has_header": key_has_header,
        "match_fields_main": main_fields,
        "match_fields_key": key_fields,
        "case_insensitive": case_insensitive,
        "strip_whitespace": strip_ws,
        "zero_pad_widths": pad_widths or [],
    }

    try:
        Path(counts_path).write_text(_format_report(result), encoding="utf-8")
    except Exception:
        pass

    logger.info("\n" + _format_report(result))
    shared_data["output_file"] = str(output_path)
    return result


# ------------------------------
# Reporting
# ------------------------------

def _format_report(r: Dict[str, Any]) -> str:
    pad = " " * 2
    lines = []
    lines.append("OPTION 12 — MERGE BY KEY — CONTROL REPORT")
    lines.append("")
    lines.append(f"Main File      : {r.get('main_file','')}")
    lines.append(f"Key File       : {r.get('key_file','')}")
    lines.append(f"Output File    : {r.get('output_file','')}")
    lines.append(f"Counts Report  : {r.get('counts_report_path','')}")
    lines.append("")
    lines.append(f"Join mode      : {r.get('join_mode','inner')}")
    lines.append(f"Main delimiter : {r.get('main_delimiter',',')}")
    lines.append(f"Key delimiter  : {r.get('key_delimiter',',')}")
    lines.append("")
    mf = r.get("match_fields_main") or []
    kf = r.get("match_fields_key") or []
    lines.append("MATCH FIELDS:")
    for i, (a,b) in enumerate(zip(mf, kf), start=1):
        lines.append(f"{pad}{i}) MAIN[{a}] ↔ KEY[{b}]")
    lines.append("")
    lines.append("TOTALS:")
    lines.append(f"{pad}Rows read (MAIN)      : {r.get('records_read',0)}")
    lines.append(f"{pad}Rows written (OUTPUT) : {r.get('records_written',0)}")
    return "\n".join(lines) + "\n"
