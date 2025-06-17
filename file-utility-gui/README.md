
# Multi-Function File Utility – GUI Version

**Version:** 1.0  
**Last Updated:** June 14, 2025  
**Author:** Marc DeJong  
**Environment:** Python 3.12+, Tkinter, Dask, Pandas

---

## 📌 Overview

This is a modular, GUI-based system for advanced CSV/TXT file processing. The tool supports both low-memory stream-based and full-file Dask processing modes. No coding is required — all functions are accessible via point-and-click GUI prompts.

---

## ✅ Supported Functions

| Menu # | Function                              | Description |
|--------|----------------------------------------|-------------|
| 1      | `filter_omit`                          | Remove records matching filter rules |
| 2      | `filter_select`                        | Keep records matching filter rules |
| 3      | `replace_rec_contents`                 | Replace values in selected fields |
| 4      | `add_rec_stub_(fixed)`                 | Add new column with fixed value |
| 5      | `add_rec_stub_(var_from_rec_contents)` | Add column based on existing fields |
| 6      | `delete_rec_by_condition`              | Remove records by rule |
| 7      | `sort_records`                         | Sort records by one or more fields |
| 8      | `dedupe_records`                       | Remove or extract duplicates |
| 9      | `split_file_by_condition`              | Split file based on one field match |
| 10     | `split_by_composite_condition`         | Split file based on multiple field matches |
| 11     | `concatenate_files`                    | Combine two or more files |
| 12     | `merge_by_key`                         | Retain/exclude records based on key file |

---

## 📂 File Structure

```
file-utility-gui/
├── 0x_File_Util_Caller_gui.py        # Main controller
├── 2x_File_Loader_gui.py             # File reader/writer logic
├── filters_3x_gui.py, filters_ui_3x_gui.py
├── 4x_Replacer_gui.py, 4x_Replacer_UI_gui.py
├── 5x_Add_Stub_gui.py, 5x_Add_Stub_UI_gui.py
├── 6x_Deleter.py
├── 7x_Sorter.py
├── 8x_Deduper.py
├── 9x_Splitter.py, splitter_9x_gui.py, splitter_9x_composite_gui.py
├── 10x_Merger.py, merger_gui_10x.py
├── 11x_Utils.py
├── requirements.txt
└── File_Utility_GUI_ReadMe.docx
```

---

## 💻 How to Run

1. Clone or download the repo  
2. Run the main script:

```
python 0x_File_Util_Caller_gui.py
```

3. Use the GUI to:
   - Load file
   - Select functions
   - Configure options
   - Run and save results

---

## ⚙ Requirements

```
pandas>=1.5.0
dask[complete]>=2023.0.0
```

Install via:

```
pip install -r requirements.txt
```

---

## 🚧 Future Enhancements

- Config save/load
- Dry-run preview
- Log file summary
- Output directory selector
- Fuzzy merge field mapping

---

## 📜 License

This project is licensed under the MIT License.
