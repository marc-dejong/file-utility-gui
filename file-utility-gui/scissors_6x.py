
"""
scissors_6x.py - (core scissors program)
Option 6 — Scissors (Column Chooser) — Stream MVP

Public entry points:
  • prepare_preview(config, logger) -> preview_payload
  • run(config, logger, shared_data) -> result_payload

This module projects a subset of columns from an input CSV/TXT into a new file,
preserving the selected order. It mirrors the structure of concatenator_11x.py
for consistency and uses simple streaming I/O.

Config keys (expected):
  - input_path: str (required)
  - delimiter: str (default ",")
  - has_headers: bool (default True)
  - selected_columns: List[str]  # names (when has_headers=True)
  - selected_indices: List[int]  # 0-based indices (when has_headers=False)
  - case_insensitive: bool (default True)
  - preserve_input_order: bool (default False)  # UI convenience flag
  - output_path: str (optional; defaults to <input>_RESULTS.csv)

Returns structured payloads; never terminates the process. Logs via the passed logger ("scissors").
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
import csv
from pathlib import Path
import io
import unicodedata

# ------------------------------
# Utility helpers (internal)
# ------------------------------

_EMPTY_CHARS = dict.fromkeys(map(ord, "\ufeff\u00a0\u200b\u200c\u200d"), None)  # BOM, NBSP, ZW*, etc.

def _norm_cell(s: str) -> str:
    if s is None:
        return ""
    s = s.translate(_EMPTY_CHARS)
    s = unicodedata.normalize("NFKC", s)
    return s.strip()


def _norm_tokens(tokens: List[str]) -> List[str]:
    return [_norm_cell(t) for t in (tokens or [])]


def _probe_encoding(path: Path) -> str:
    """Light-weight probe; try utf-8 then fall back to cp1252."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            f.read(4096)
        return "utf-8"
    except Exception:
        return "cp1252"


def _iter_lines(path: Path, encoding: str) -> io.TextIOWrapper:
    return open(path, "r", encoding=encoding, newline="")


def _read_first_row(path: Path, delimiter: str, encoding: str) -> List[str]:
    with _iter_lines(path, encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            row = _norm_tokens(row)
            if row and any(row):
                return row
    return []


def _map_selected_to_indices(header: List[str], selected_columns: List[str], case_insensitive: bool) -> Tuple[List[int], List[str]]:
    idxs: List[int] = []
    missing: List[str] = []
    if not header:
        return idxs, selected_columns[:]  # all missing if no header

    # Build lookup
    if case_insensitive:
        lookup = {h.lower(): i for i, h in enumerate(header)}
        for name in selected_columns:
            key = (name or "").lower()
            if key in lookup:
                idxs.append(lookup[key])
            else:
                missing.append(name)
    else:
        lookup = {h: i for i, h in enumerate(header)}
        for name in selected_columns:
            if name in lookup:
                idxs.append(lookup[name])
            else:
                missing.append(name)
    return idxs, missing


def _safe_writer(out_path: Path, delimiter: str):
    """Prefer robust CSV helpers if available; otherwise fallback to csv.writer.
    NOTE: You can swap in csv_utils.write_scsv here if desired.
    """
    # try:
    #     from csv_utils import write_scsv
    #     return write_scsv(open(out_path, "w", encoding="utf-8", newline=""))
    # except Exception:
    return csv.writer(open(out_path, "w", encoding="utf-8", newline=""), delimiter=delimiter)


# ------------------------------
# Public: Preview
# ------------------------------

def prepare_preview(config: Dict[str, Any], logger) -> Dict[str, Any]:
    input_path = Path(config.get("input_path", "")).expanduser()
    delimiter = config.get("delimiter", ",")
    has_headers = bool(config.get("has_headers", True))
    case_insensitive = bool(config.get("case_insensitive", True))

    logger.info("scissors_6x VERSION: 2025-08-28-a")

    if not input_path or not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    enc = _probe_encoding(input_path)
    first_row = _read_first_row(input_path, delimiter, enc)
    if not first_row:
        raise ValueError("Input appears empty or unreadable.")

    if has_headers:
        header = first_row
    else:
        header = [f"Col {i+1}" for i in range(len(first_row))]

    preview = {
        "input_path": str(input_path),
        "encoding": enc,
        "delimiter": delimiter,
        "has_headers": has_headers,
        "header": header,            # display tokens
        "column_count": len(header),
        "case_insensitive": case_insensitive,
    }
    logger.info(f"[Preview] Columns detected: {len(header)}")
    return preview


# ------------------------------
# Public: Run
# ------------------------------

def run(config: Dict[str, Any], logger, shared_data: Dict[str, Any]) -> Dict[str, Any]:
    input_path = Path(config.get("input_path", "")).expanduser()
    delimiter = config.get("delimiter", ",")
    has_headers = bool(config.get("has_headers", True))
    case_insensitive = bool(config.get("case_insensitive", True))

    # Determine output path
    output_path = Path(config.get("output_path") or (input_path.with_name(input_path.stem + "_RESULTS.csv")))
    counts_report_path = output_path.with_name(output_path.stem + "_COUNTS.txt")

    selected_columns: List[str] = config.get("selected_columns") or []
    selected_indices: List[int] = config.get("selected_indices") or []

    warnings: List[str] = []
    errors: List[str] = []
    total_rows_written = 0
    duplicate_selections = 0

    if not input_path or not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    enc = _probe_encoding(input_path)

    # Determine header and mapping
    with _iter_lines(input_path, enc) as f:
        reader = csv.reader(f, delimiter=delimiter)
        header_tokens: List[str] = []
        if has_headers:
            for row in reader:
                row = _norm_tokens(row)
                if row and any(row):
                    header_tokens = row
                    break
            if not header_tokens:
                raise ValueError("Could not read header row from input file.")

            if selected_columns:
                idxs, missing = _map_selected_to_indices(header_tokens, selected_columns, case_insensitive)
            elif selected_indices:
                idxs, missing = selected_indices[:], []
            else:
                raise ValueError("No columns selected.")

            if missing:
                warnings.append(f"Missing columns (skipped): {', '.join(missing)}")

            # Compute display names in chosen order
            out_header = [header_tokens[i] for i in idxs]

        else:
            # No headers: first non-empty row defines width
            first_row: List[str] = []
            for row in reader:
                row = _norm_tokens(row)
                if row and any(row):
                    first_row = row
                    break
            if not first_row:
                raise ValueError("Input appears to have no data rows.")

            width = len(first_row)
            if not selected_indices and selected_columns:
                # If UI passed names in no-header mode, interpret as 1-based indices in string form
                try:
                    selected_indices = [int(x) - 1 for x in selected_columns]
                except Exception:
                    raise ValueError("In no-header mode, selected columns must be indices (1-based or 0-based).")

            if not selected_indices:
                raise ValueError("No column indices selected.")

            # Bound-check & sanitize
            idxs = []
            for i in selected_indices:
                if 0 <= i < width:
                    idxs.append(i)
                else:
                    warnings.append(f"Index out of range skipped: {i}")
            out_header = []  # no header written in no-header mode

        # Duplicate selection count (purely informational)
        duplicate_selections = len(idxs) - len(set(idxs)) if idxs else 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream rows and project
    with _iter_lines(input_path, enc) as f_in:
        reader = csv.reader(f_in, delimiter=delimiter)
        # Writer (favor robust helper if you swap it in)
        with open(output_path, "w", encoding="utf-8", newline="") as out_f:
            writer = csv.writer(out_f, delimiter=delimiter)

            wrote_header = False
            for row in reader:
                row = _norm_tokens(row)
                if not row or not any(row):
                    continue

                if has_headers and not wrote_header:
                    # First non-empty row is the header; write selected subset header
                    # (We already mapped idxs using this row earlier.)
                    if out_header:
                        writer.writerow(out_header)
                    wrote_header = True
                    continue  # move to first data row

                # Project
                proj = [row[i] if i < len(row) else "" for i in idxs]
                try:
                    writer.writerow(proj)
                    total_rows_written += 1
                except Exception as e:
                    warnings.append(f"Failed to write a row: {e}")

    shared_data["output_file"] = str(output_path)

    result_payload = {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "counts_report_path": str(counts_report_path),
        "delimiter": delimiter,
        "has_headers": has_headers,
        "selected_columns": selected_columns,
        "selected_indices": selected_indices,
        "case_insensitive": case_insensitive,
        "rows_written": int(total_rows_written),
        "duplicate_selections": int(duplicate_selections),
        "warnings": warnings,
        "errors": errors,
    }

    # Write counts/control report
    try:
        _write_counts_report(result_payload, counts_report_path)
    except Exception as e:
        warnings.append(f"Failed to write counts report: {e}")

    # Console report for the calling UI/logger
    logger.info("\n" + _format_console_report(result_payload))
    return result_payload


# ------------------------------
# Reporting helpers
# ------------------------------

def _write_counts_report(result_payload: Dict[str, Any], path: Path) -> None:
    Path(path).write_text(_format_console_report(result_payload), encoding="utf-8")


def _format_console_report(result_payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    pad = " " * 2
    lines.append("OPTION 6 — SCISSORS — CONTROL REPORT")
    lines.append("")
    lines.append(f"Input File     : {result_payload.get('input_path','')}")
    lines.append(f"Output File    : {result_payload.get('output_path','')}")
    lines.append(f"Counts Report  : {result_payload.get('counts_report_path','')}")
    lines.append("")

    # Selected columns summary
    sel_cols = result_payload.get("selected_columns") or []
    sel_idxs = result_payload.get("selected_indices") or []
    if sel_cols:
        lines.append("SELECTED COLUMNS (by name):")
        for i, name in enumerate(sel_cols, start=1):
            lines.append(f"{pad}{i}) {name}")
    elif sel_idxs:
        lines.append("SELECTED COLUMNS (by index):")
        for i, idx in enumerate(sel_idxs, start=1):
            lines.append(f"{pad}{i}) index {idx}")
    lines.append("")

    # Totals
    lines.append("TOTALS:")
    lines.append(f"{pad}Rows written           : {result_payload.get('rows_written',0)}")
    lines.append(f"{pad}Duplicate selections   : {result_payload.get('duplicate_selections',0)}")

    warnings = result_payload.get("warnings") or []
    errors = result_payload.get("errors") or []
    if warnings:
        lines.append("")
        lines.append("WARNINGS:")
        for w in warnings:
            lines.append(f"{pad}- {w}")
    if errors:
        lines.append("")
        lines.append("ERRORS:")
        for e in errors:
            lines.append(f"{pad}- {e}")

    return "\n".join(lines) + "\n"
