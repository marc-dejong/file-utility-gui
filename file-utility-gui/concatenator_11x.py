"""
concatenator_11x.py — Option 11 (Concatenate Files) — Stream MVP

This module implements two public entry points used by the UI and router:
  • prepare_preview(config, logger) -> preview_payload
  • run(config, logger, shared_data) -> result_payload

It MUST NOT terminate the process; return structured payloads and log via the
named logger ("concatenator").
"""
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
import csv
import io
import os
import re
from pathlib import Path
from fnmatch import fnmatch
import unicodedata

# ------------------------------
# Utility helpers (internal)
# ------------------------------

_EMPTY_CHARS = dict.fromkeys(map(ord, "\ufeff\u00a0\u200b\u200c\u200d"), None)  # BOM, NBSP, ZW*, etc.

def _norm_tokens(tokens: list[str]) -> list[str]:
    def _clean_cell(s: str) -> str:
        if s is None:
            return ""
        # drop BOM/NBSP/zero-width, normalize, then trim
        s = s.translate(_EMPTY_CHARS)
        s = unicodedata.normalize("NFKC", s)
        return s.strip()
    return [_clean_cell(t) for t in (tokens or [])]


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff") if isinstance(s, str) else s

def _natural_key(s: str):
    """Return a key for natural sort (e.g., file2 before file10)."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", s)]

def _ext_ok(name: str, extensions: List[str]) -> bool:
    """Return True if filename matches one of the given extensions.

    Supports patterns like 'csv', '.csv', '*.csv', or full fnmatch globs.
    """
    if not extensions:
        return True
    lower = name.lower()
    for ext in extensions:
        ext = (ext or "").strip().lower()
        if not ext:
            continue
        # Normalize simple tokens
        if ext.startswith("*."):
            pattern = ext              # '*.csv'
        elif ext.startswith("."):
            pattern = f"*{ext}"        # '.csv' -> '*.csv'
        elif ext.startswith("*"):
            pattern = ext              # already glob
        else:
            pattern = f"*.{ext}"       # 'csv' -> '*.csv'
        if fnmatch(lower, pattern):
            return True
    return False



def _is_hidden_or_system(name: str) -> bool:
    lower = name.lower()
    return (
        name.startswith(".")
        or name.startswith("~")
        or lower in {"desktop.ini", "thumbs.db"}
    )


def _probe_encoding(path: Path) -> str:
    """Very light-weight probe; we do NOT import chardet. Just try utf-8 first."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            f.read(4096)
        return "utf-8"
    except Exception:
        return "cp1252"


def _iter_lines(path: Path, encoding: str) -> io.TextIOWrapper:
    return open(path, "r", encoding=encoding, newline="")


def _tokenize_row(line: str, delimiter: str) -> List[str]:
    # Fallback tokenizer for first-line peek without full csv parsing
    return [t.strip() for t in line.rstrip("\n\r").split(delimiter)] if line else []


def _read_first_nonempty_line(path: Path, encoding: str) -> str:
    with _iter_lines(path, encoding) as f:
        for line in f:
            if line.strip():
                return line
    return ""


# ------------------------------
# Public: Preview
# ------------------------------

def prepare_preview(config: Dict[str, Any], logger) -> Dict[str, Any]:
    """Build a non-destructive preview of the concatenation run.

    Expected config keys (some optional):
        - input_dir: str
        - extensions: List[str]
        - has_headers: bool
        - delimiter: str
        - header_validation: "strict" | "compatible" | "none"
        - on_mismatch: "skip" | "abort"
        - natural_sort: bool
        - skip_empty_files: bool
        - ignore_hidden_system: bool
        - preview_first_n_headers: int
    """
    # Normalize config
    input_dir = Path(config.get("input_dir", "."))
    extensions = config.get("extensions", []) or []
    has_headers = bool(config.get("has_headers", True))
    delimiter = config.get("delimiter", ",")
    header_validation = (config.get("header_validation") or "strict").lower()
    on_mismatch = (config.get("on_mismatch") or "skip").lower()
    natural_sort = bool(config.get("natural_sort", True))
    skip_empty = bool(config.get("skip_empty_files", True))
    ignore_hidden = bool(config.get("ignore_hidden_system", True))

    logger.info("concatenator_11x VERSION: 2025-08-27-b")

    files_scanned: List[Dict[str, Any]] = _scan_files({
        "input_dir": str(input_dir),
        "extensions": extensions,
        "ignore_hidden_system": ignore_hidden,
        "natural_sort": natural_sort,
        "delimiter": delimiter,
        "has_headers": has_headers,
        "skip_empty_files": skip_empty,
    })

    warnings: List[str] = []
    include_files: List[str] = []
    skip_files: List[Dict[str, str]] = []

    # Choose reference header if needed
    ref = _choose_reference_header(files_scanned, header_validation) if has_headers else None

    # Decide which files to include/skip
    for fs in files_scanned:
        fname = fs["filename"]
        if fs.get("empty", False) and skip_empty:
            skip_files.append({"filename": fname, "reason": "empty"})
            continue
        if extensions and not _ext_ok(fname, extensions):
            skip_files.append({"filename": fname, "reason": "extension"})
            continue
        if ignore_hidden and fs.get("is_hidden"):
            skip_files.append({"filename": fname, "reason": "hidden/system"})
            continue
        if has_headers and header_validation != "none":
            tokens = fs.get("first_row_tokens") or []
            if tokens and not _headers_equal(tokens, ref["tokens"] if ref else [], header_validation):
                if on_mismatch == "abort":
                    warnings.append(f"Header mismatch in {fname}; run is configured to abort on mismatch.")
                skip_files.append({"filename": fname, "reason": "header_mismatch"})
                continue
        include_files.append(fname)

    # Estimated row total is unknown without reading; best-effort: None
    preview = {
        "files_scanned": files_scanned,
        "reference_header": ref,
        "include_files": include_files,
        "skip_files": skip_files,
        "estimated_row_total": None,
        "warnings": warnings,
        "header_validation": header_validation,
        "on_mismatch": on_mismatch,
    }

    logger.info(f"[Preview] Scanned {len(files_scanned)} files; will include {len(include_files)} and skip {len(skip_files)}.")
    return preview


# ------------------------------
# Public: Run
# ------------------------------

def run(config: Dict[str, Any], logger, shared_data: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the concatenation, writing artifacts and returning run totals."""
    input_dir = Path(config.get("input_dir", "."))
    delimiter = config.get("delimiter", ",")
    has_headers = bool(config.get("has_headers", True))
    header_validation = (config.get("header_validation") or "strict").lower()
    on_mismatch = (config.get("on_mismatch") or "skip").lower()
    ignore_hidden = bool(config.get("ignore_hidden_system", True))
    natural_sort = bool(config.get("natural_sort", True))
    skip_empty = bool(config.get("skip_empty_files", True))
    extensions = config.get("extensions", []) or []

    # Output paths
    output_path = Path(config.get("output_path") or (input_dir / f"{input_dir.name}_RESULTS.csv"))
    counts_report_path = Path(config.get("counts_report_path") or (output_path.with_name(output_path.stem + "_COUNTS.txt")))
    dropped_records_path = Path(config.get("dropped_records_path") or (output_path.with_name(output_path.stem + "_DROPPED_RECORDS.csv")))

    # Scan files
    files_scanned = _scan_files({
        "input_dir": str(input_dir),
        "extensions": extensions,
        "ignore_hidden_system": ignore_hidden,
        "natural_sort": natural_sort,
        "delimiter": delimiter,
        "has_headers": has_headers,
        "skip_empty_files": skip_empty,
    })

    # Determine reference header
    ref = _choose_reference_header(files_scanned, header_validation) if has_headers else None

    files_included: List[Dict[str, Any]] = []
    files_skipped: List[Dict[str, str]] = []
    warnings: List[str] = []
    errors: List[str] = []
    headers_suppressed_total = 0
    total_rows_written = 0
    dropped_total = 0
    encoding_fallbacks: List[Dict[str, str]] = []

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use context managers for both writers
    with open(output_path, "w", encoding="utf-8", newline="") as output_file, \
         open(dropped_records_path, "w", encoding="utf-8", newline="") as dropped_f:

        writer = csv.writer(output_file, delimiter=delimiter)
        dropped_writer = csv.writer(dropped_f, delimiter=delimiter)

        # Write header once if present
        if has_headers and ref and ref.get("tokens"):
            writer.writerow(ref["tokens"])
            dropped_writer.writerow(ref["tokens"])  # mirror header in dropped file
            shared_data["output_header"] = ref["tokens"]

        # Iterate files and append (your existing loop, unchanged)
        for fs in files_scanned:
            fname = fs["filename"]
            if fs.get("empty", False) and skip_empty:
                files_skipped.append({"filename": fname, "reason": "empty"})
                continue
            if extensions and not _ext_ok(fname, extensions):
                files_skipped.append({"filename": fname, "reason": "extension"})
                continue
            if ignore_hidden and fs.get("is_hidden"):
                files_skipped.append({"filename": fname, "reason": "hidden/system"})
                continue

            if has_headers and header_validation != "none":
                tokens = fs.get("first_row_tokens") or []
                if tokens and not _headers_equal(tokens, ref["tokens"] if ref else [], header_validation):
                    msg = f"Header mismatch in {fname}"
                    if on_mismatch == "abort":
                        errors.append(msg + "; abort on mismatch is set.")
                        break
                    else:
                        files_skipped.append({"filename": fname, "reason": "header_mismatch"})
                        continue

            # Append file (this still re-opens the output in append mode inside)
            rows_read, rows_written, rows_dropped = _stream_append_file(fs, ref, config, logger)
            total_rows_written += rows_written
            dropped_total += rows_dropped
            headers_suppressed_total += (1 if has_headers and rows_read > 0 else 0)
            files_included.append({"filename": fname, "rows_appended": rows_written})

    # <-- files auto-closed here

    # Close writers
    output_file.close()
    dropped_f.close()

    # Update shared_data
    shared_data["output_file"] = str(output_path)

    result_payload = {
        "output_path": str(output_path),
        "files_included": files_included,
        "files_skipped": files_skipped,
        "total_rows_written": int(total_rows_written),
        "dropped_total": int(dropped_total),
        "headers_suppressed_total": int(headers_suppressed_total),
        "encoding_fallbacks": encoding_fallbacks,
        "counts_report_path": str(counts_report_path) if counts_report_path else None,
        "dropped_records_path": str(dropped_records_path) if dropped_records_path else None,
        "warnings": warnings,
        "errors": errors,
    }

    # Write counts/control report
    try:
        _write_counts_report(result_payload, str(counts_report_path))
    except Exception as e:
        warnings.append(f"Failed to write counts report: {e}")

    # Console report for the calling UI/logger
    logger.info("\n" + _format_console_report(result_payload))
    return result_payload


# ------------------------------
# Internal helpers
# ------------------------------

def _scan_files(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return lightweight file summaries for preview.

    Each summary may include: filename, size, is_hidden, encoding_probe, first_row_tokens, col_count, empty.
    """
    input_dir = Path(config.get("input_dir", "."))
    delimiter = config.get("delimiter", ",")
    has_headers = bool(config.get("has_headers", True))
    ignore_hidden = bool(config.get("ignore_hidden_system", True))
    natural_sort = bool(config.get("natural_sort", True))

    # Collect candidate files
    entries = [p for p in input_dir.iterdir() if p.is_file()]
    entries.sort(key=(lambda p: _natural_key(p.name)) if natural_sort else None)

    file_summaries: List[Dict[str, Any]] = []
    for p in entries:
        enc = _probe_encoding(p)
        size = p.stat().st_size
        is_hidden = _is_hidden_or_system(p.name)
        empty = size == 0
        first_row_tokens: List[str] | None = None
        col_count: Optional[int] = None

        if not empty:
            try:
                first_line = _read_first_nonempty_line(p, enc)
                if first_line:
                    first_row_tokens = _tokenize_row(first_line, delimiter)
                    col_count = len(first_row_tokens)
            except Exception:
                # If we cannot peek, leave tokens None; downstream may still try to read
                pass

        file_summaries.append({
            "filename": p.name,
            "path": str(p),
            "size": int(size),
            "is_hidden": bool(is_hidden),
            "encoding_probe": enc,
            "first_row_tokens": first_row_tokens,
            "col_count": col_count,
            "empty": empty,
        })

    return file_summaries


def _choose_reference_header(file_summaries: List[Dict[str, Any]], mode: str) -> Optional[Dict[str, Any]]:
    mode = (mode or "strict").lower()
    if not file_summaries:
        return None

    # Pick the first file that has tokenized first row
    for fs in file_summaries:
        tokens = fs.get("first_row_tokens")
        if tokens:
            return {"filename": fs["filename"], "tokens": tokens}
    return None

def _headers_equal(a: List[str], b: List[str], mode: str) -> bool:
    mode = (mode or "strict").lower()
    if mode == "none":
        return True
    if a is None or b is None:
        return False

    a2, b2 = _norm_tokens(a), _norm_tokens(b)

    if mode == "strict":
        return a2 == b2

    if len(a2) != len(b2):
        return False
    for x, y in zip(a2, b2):
        if x.lower() != y.lower():
            return False
    return True


def _is_repeated_header(tokens: List[str], ref: Optional[Dict[str, Any]], mode: str) -> bool:
    if not ref or not tokens:
        return False
    return _headers_equal(tokens, ref.get("tokens") or [], mode)


def _stream_append_file(file_summary: Dict[str, Any], ref_header: Optional[Dict[str, Any]],
                        config: Dict[str, Any], logger) -> Tuple[int, int, int]:
    """Append one file's data rows to the open writer (managed by caller).

    NOTE: This helper does not directly write; instead it streams and writes here to the global
    output via csv.writer instantiated in run(). For simplicity in this MVP, we open and write
    within this function (keeps state self-contained). It returns counts for the caller.
    """
    path = Path(file_summary["path"])
    delimiter = config.get("delimiter", ",")
    has_headers = bool(config.get("has_headers", True))
    header_validation = (config.get("header_validation") or "strict").lower()

    rows_read_data = 0
    rows_written = 0
    rows_dropped = 0
    ref_len = len(ref_header["tokens"]) if (ref_header and ref_header.get("tokens")) else None
    encoding = file_summary.get("encoding_probe") or _probe_encoding(path)

    # We'll write directly to the results file by re-opening in append mode to avoid passing writer
    # (The run() function wrote the initial header already.)
    out_path = Path(config.get("output_path") or (Path(config.get("input_dir", ".")) / f"{Path(config.get('input_dir','.') ).name}_RESULTS.csv"))
    with open(out_path, "a", encoding="utf-8", newline="") as outf:
        writer = csv.writer(outf, delimiter=delimiter)
        with _iter_lines(path, encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            first = True
            for row in reader:
                # Normalize first so emptiness checks see truly empty cells
                row = _norm_tokens(row)

                # Skip blank/whitespace-only rows *after* normalization
                if not row or not any(row):
                    continue

                # DEBUG: log the first 3 non-blank rows we see per file (post-normalization)
                try:
                    debug_seen
                except NameError:
                    debug_seen = 0
                if debug_seen < 3:
                    logger.info(
                        f"[DEBUG] {path.name}: row{debug_seen + 1}={row!r} "
                        f"len={len(row)} populated={sum(1 for c in row if c)}"
                    )
                    debug_seen += 1

                # Enforce column count if a reference header exists
                if ref_len is not None and len(row) != ref_len:
                    rows_dropped += 1
                    continue

                # Drop “almost empty” rows (e.g., '', '', '', '2025')
                # (_norm_tokens() has already run; empty cells are '')
                populated = sum(1 for c in row if c)
                if populated <= 1:
                    rows_dropped += 1
                    continue

                # Extra guard: if 4 columns and ONLY the Year column has a value, drop it
                if ref_len == 4 and (not row[0] and not row[1] and not row[2]) and row[3].isdigit() and len(
                        row[3]) >= 4:
                    rows_dropped += 1
                    continue

                # Drop stray Year-only line that sneaks through
                if len(row) == 1 and row[0].isdigit() and len(row[0]) == 4:
                    rows_dropped += 1
                    continue

                rows_read_data += 1
                try:
                    writer.writerow(row)
                    rows_written += 1
                except Exception:
                    rows_dropped += 1

    return rows_read_data, rows_written, rows_dropped


def _write_counts_report(result_payload: Dict[str, Any], path: str) -> None:
    text = _format_console_report(result_payload)
    Path(path).write_text(text, encoding="utf-8")


def _format_console_report(result_payload: Dict[str, Any]) -> str:
    """Return a monospaced, human-readable control report string for console output."""
    lines = []
    pad = " " * 2
    lines.append("OPTION 11 — CONCATENATE FILES — CONTROL REPORT")
    lines.append("")
    lines.append(f"Output File    : {result_payload.get('output_path','')}")
    lines.append(f"Dropped File   : {result_payload.get('dropped_records_path','')}")
    lines.append(f"Counts Report  : {result_payload.get('counts_report_path','')}")
    lines.append("")

    # Files included
    lines.append("FILES INCLUDED:")
    if result_payload.get("files_included"):
        for item in result_payload["files_included"]:
            lines.append(f"{pad}{item['filename']}  (rows appended: {item.get('rows_appended',0)})")
    else:
        lines.append(f"{pad}<none>")
    lines.append("")

    # Files skipped
    lines.append("FILES SKIPPED:")
    if result_payload.get("files_skipped"):
        for item in result_payload["files_skipped"]:
            lines.append(f"{pad}{item['filename']}  — {item.get('reason','')}")
    else:
        lines.append(f"{pad}<none>")
    lines.append("")

    # Totals
    lines.append("TOTALS:")
    lines.append(f"{pad}Total rows written     : {result_payload.get('total_rows_written',0)}")
    lines.append(f"{pad}Dropped rows           : {result_payload.get('dropped_total',0)}")
    lines.append(f"{pad}Header lines omitted   : {result_payload.get('headers_suppressed_total',0)}")

    # Warnings/Errors
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
