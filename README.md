# Multi-Function File Utility — GUI Version

**Version:** 2.0  
**Release:** 2025-08-28-a  
**Last Updated:** August 28, 2025  
**Author:** Marc DeJong  
**Environment:** Python 3.12+, Tkinter; pandas & dask (for selected features)

[![Documentation Status](https://readthedocs.org/projects/file-utility-gui/badge/?version=latest)](https://file-utility-gui.readthedocs.io/en/latest/)
---

## 📌 Overview

A modular, GUI-based toolkit for working with structured CSV/TXT files. It supports both **low-memory streaming operations** and **full-file (Dask)** operations, selectable per function.  
All features are accessible through the graphical interface — no coding required.

The main entry point is:
```bash
python file_util_gui_0x.py
```

---

## ✅ Supported GUI Functions

| #  | Function Name | Description |
|----|----------------|-------------|
| 1  | `filter_omit` | Remove records matching rules |
| 2  | `filter_select` | Keep records matching rules |
| 3  | `replace_rec_contents` | Replace values via rules |
| 4  | `add_rec_stub_(fixed)` | Add a column with a fixed value |
| 5  | `add_rec_stub_(var_from_rec_contents)` | Add a column derived from other fields |
| 6  | `Scissors — Column Chooser` | Pick/arrange columns; writes report |
| 7  | `sort_records` | Sort by one or more fields |
| 8  | `dedupe_records` | Remove or extract duplicates |
| 9  | `split_by_key (Match / No Match)` | NEW: Split MAIN by presence in KEY |
| 10 | `split_by_composite_condition` | Split by multiple field conditions |
| 11 | `concatenate_files` | Combine files vertically (fast-path) |
| 12 | `merge_by_key` | Merge/Roll-in by key (fast-path) |
| 13 | `compare_two_files_gui` | Compare two files; write compare report |

---

## 🆕 What's New in Release 2025-08-28-a

### Option 9 — Split by Key (Match / No Match)
**Modules:** `key_split_9x.py`, `key_split_ui_9x_gui.py`

- Builds a key set from a **KEY file**, then streams the **MAIN file** once.
- Writes each row to **MATCHED** or **NONMATCHED** outputs.  
- Normalization options: case-insensitive, whitespace-strip, auto zero-pad widths from KEY.  
- Encoding probe with UTF-8 fallback to cp1252.  
- Outputs:  
  - `<main>_MATCHED.csv`  
  - `<main>_NONMATCHED.csv`  
  - `<main>_SPLIT_COUNTS.txt` (summary report)

**Performance Notes:**  
- MAIN is streamed in a single pass; memory use scales with KEY size.  
- Ideal for very large MAIN with smaller KEY.

---

## 🧩 Routing & UX Updates

- Fast-path auto-launch for functions #11 and #12.  
- Option 9 fully integrated into controller.  
- Improved label rendering and delimiter auto-detection.  
- Window centering and UI clamping for consistent display.  

---

## 💻 How to Run

Ensure all scripts are in the same folder, then launch the main GUI:
```bash
python file_util_gui_0x.py
```

### Quick Start (Option 9 Example)
1. Select **Main file** and **Key file**.  
2. Click **Preview headers**.  
3. Select matching fields on each side and **Add Pair**.  
4. (Optional) Override output filenames.  
5. Click **Run Now** to process.  

A summary dialog appears; totals are also saved to the `_SPLIT_COUNTS.txt` file.

---

## 📂 Folder & Module Structure

| File | Purpose |
|------|----------|
| `file_util_gui_0x.py` | Main controller (routing + fast-paths) |
| `key_split_9x.py` | Engine for Option 9 (streamed split by key) |
| `key_split_ui_9x_gui.py` | GUI for Option 9 (mapping, options, run) |
| `merger_gui_10x.py` | Merge GUI / engine |
| `concatenate_ui_11x_gui.py` | Concatenate GUI |
| `Two_File_Compare_gui.py` | Standalone Compare GUI |

---

## ⚙ Requirements

```bash
pip install pandas dask
```

Python 3.12+ with Tkinter (bundled).

---

## ⚠ Known Limits & Tips

- If header preview shows a single long cell, re-run with **Auto-detect Delimiter**.  
- Memory scales with unique KEY combinations — keep key columns minimal.  
- Ensure “Has header” is set correctly for both files to avoid misalignment.  

---

## 📜 License

Licensed under the MIT License.  

---

## 📅 Release History

| Release | Summary |
|----------|----------|
| **2025-08-28-a** | New Option 9 (Split by Key); 0x integration & UI fixes |
| **2025-06-14** | Initial GUI ReadMe, functions 1–13 |
