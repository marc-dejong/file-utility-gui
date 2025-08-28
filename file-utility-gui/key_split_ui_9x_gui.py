"""
key_split_ui_9x_gui.py — Option 9 GUI (Split by Key: Match / No Match)
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import csv  # used for sniff fallback

# Local engine
import key_split_9x as engine  # prepare_preview, run, _probe_encoding

# ------------------------------
# Theme / style helpers
# ------------------------------

def _init_theme_and_styles():
    """Force a consistent ttk theme and visible label color on Windows."""
    style = ttk.Style()
    # Prefer 'vista' on Windows; fall back to 'clam' else.
    for theme in ("vista", "clam", style.theme_use()):
        try:
            style.theme_use(theme)
            break
        except Exception:
            continue
    # Ensure labels are visible even on light/hover edge cases
    style.configure("FormLabel.TLabel", foreground="black")
    style.configure("FormCheck.TCheckbutton")
    style.configure("FormButton.TButton")

# ------------------------------
# Small helpers
# ------------------------------

DELIM_CHOICES = [
    (",", "Comma (,)"),
    ("|", "Pipe (|)"),
    ("\t", "Tab (\\t)"),
    (";", "Semicolon (;)"),
]

def _fmt_field_label(idx: int, name: str) -> str:
    name = name or f"Col{idx}"
    return f"[{idx}] {name}"

def _sniff_delimiter(path: Path, default: str = ",") -> str:
    try:
        enc = engine._probe_encoding(path)
        with open(path, "r", encoding=enc, newline="") as f:
            sample = f.read(4096)
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "|", "\t", ";"])
        return dialect.delimiter or default
    except Exception:
        return default

def _peek_width(path: Path, delimiter: str, has_header: bool, *, logger=None) -> int:
    try:
        enc = engine._probe_encoding(path)
        with open(path, "r", encoding=enc, newline="") as f:
            r = csv.reader(f, delimiter=delimiter)
            row = next(r, [])
            if has_header:
                row2 = next(r, row)
                return max(len(row), len(row2))
            return len(row)
    except Exception as e:
        if logger:
            logger.warning(f"[9x UI] _peek_width failed for {path}: {e}")
        return 0

def _parse_int_list(text: str) -> Optional[List[int]]:
    if not text:
        return None
    try:
        return [int(x.strip()) for x in text.split(",") if x.strip() != ""]
    except Exception:
        return None

# ------------------------------
# Dialog
# ------------------------------

class KeySplitConfigDialog(tk.Toplevel):
    def __init__(self, master, shared_data: Optional[Dict[str, Any]], logger):
        super().__init__(master)
        _init_theme_and_styles()
        self.title("Option 9 — Split by Key (Match / No Match)")
        self.resizable(True, True)

        self.shared_data = shared_data or {}
        self.logger = logger

        # State vars
        self.var_main_path = tk.StringVar()
        self.var_key_path  = tk.StringVar()

        self.var_main_delim = tk.StringVar(value=",")
        self.var_key_delim  = tk.StringVar(value=",")

        self.var_main_has_header = tk.BooleanVar(value=True)
        self.var_key_has_header  = tk.BooleanVar(value=True)
        self.var_write_header    = tk.BooleanVar(value=True)

        self.var_case_insensitive = tk.BooleanVar(value=True)
        self.var_strip_ws         = tk.BooleanVar(value=True)
        self.var_auto_zpad        = tk.BooleanVar(value=True)

        self.var_manual_pads = tk.StringVar(value="")  # optional

        self.preview_main_header: List[str] = []
        self.preview_key_header: List[str] = []
        self.mappings: List[Tuple[int, int]] = []

        self._return_mode = "cancel"
        self._result_config: Optional[Dict[str, Any]] = None
        self._result_payload: Optional[Dict[str, Any]] = None

        self._build_ui()
        self._wire_events()

        # Position (shift slightly to the right; clamp on-screen)
        self.update_idletasks()
        try:
            self.geometry(self._center_geometry(master))
        except Exception:
            pass

        self.grab_set()
        self.transient(master)

    # ---------- UI construction ----------

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        # Files frame
        frm_files = ttk.LabelFrame(self, text="Files")
        frm_files.grid(row=0, column=0, columnspan=2, sticky="nsew", **pad)

        ttk.Label(frm_files, text="Main file:", style="FormLabel.TLabel").grid(row=0, column=0, sticky="w")
        ent_main = ttk.Entry(frm_files, textvariable=self.var_main_path, width=76)
        ent_main.grid(row=0, column=1, sticky="ew", padx=(6, 6))
        ttk.Button(frm_files, text="Browse...", command=self._choose_main, style="FormButton.TButton").grid(row=0, column=2, sticky="e")

        ttk.Label(frm_files, text="Key file:", style="FormLabel.TLabel").grid(row=1, column=0, sticky="w")
        ent_key = ttk.Entry(frm_files, textvariable=self.var_key_path, width=76)
        ent_key.grid(row=1, column=1, sticky="ew", padx=(6, 6))
        ttk.Button(frm_files, text="Browse...", command=self._choose_key, style="FormButton.TButton").grid(row=1, column=2, sticky="e")

        frm_files.columnconfigure(1, weight=1)

        # Delimiters & headers
        frm_opts = ttk.LabelFrame(self, text="Delimiters & Header Options")
        frm_opts.grid(row=1, column=0, sticky="nsew", **pad)

        ttk.Label(frm_opts, text="Main delimiter:", style="FormLabel.TLabel").grid(row=0, column=0, sticky="w")
        cmb_main = ttk.Combobox(frm_opts, textvariable=self.var_main_delim, width=18,
                                values=[d for d, _ in DELIM_CHOICES], state="readonly")
        cmb_main.grid(row=0, column=1, sticky="w", padx=(4, 12))
        ttk.Button(frm_opts, text="Auto-detect", command=self._auto_detect_main, style="FormButton.TButton").grid(row=0, column=2, sticky="w")

        ttk.Label(frm_opts, text="Key delimiter:", style="FormLabel.TLabel").grid(row=1, column=0, sticky="w")
        cmb_key = ttk.Combobox(frm_opts, textvariable=self.var_key_delim, width=18,
                               values=[d for d, _ in DELIM_CHOICES], state="readonly")
        cmb_key.grid(row=1, column=1, sticky="w", padx=(4, 12))
        ttk.Button(frm_opts, text="Auto-detect", command=self._auto_detect_key, style="FormButton.TButton").grid(row=1, column=2, sticky="w")

        ttk.Checkbutton(frm_opts, text="Main has header", variable=self.var_main_has_header, style="FormCheck.TCheckbutton",
                        command=self._sync_write_header_default).grid(row=2, column=0, sticky="w")
        ttk.Checkbutton(frm_opts, text="Key has header", variable=self.var_key_has_header, style="FormCheck.TCheckbutton").grid(row=2, column=1, sticky="w")
        ttk.Checkbutton(frm_opts, text="Write header to outputs", variable=self.var_write_header, style="FormCheck.TCheckbutton").grid(row=2, column=2, sticky="w")

        # Normalization frame
        frm_norm = ttk.LabelFrame(self, text="Normalization & Matching Options")
        frm_norm.grid(row=1, column=1, sticky="nsew", **pad)

        ttk.Checkbutton(frm_norm, text="Case-insensitive", variable=self.var_case_insensitive, style="FormCheck.TCheckbutton").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(frm_norm, text="Strip whitespace", variable=self.var_strip_ws, style="FormCheck.TCheckbutton").grid(row=1, column=0, sticky="w")
        ttk.Checkbutton(frm_norm, text="Auto zero-pad (learn widths from KEY)", variable=self.var_auto_zpad, style="FormCheck.TCheckbutton").grid(row=2, column=0, sticky="w")

        ttk.Label(frm_norm, text="Manual zero-pad widths (comma-separated; optional):", style="FormLabel.TLabel").grid(row=3, column=0, sticky="w", pady=(8,2))
        ttk.Entry(frm_norm, textvariable=self.var_manual_pads, width=36).grid(row=4, column=0, sticky="w")

        # Preview + mapping
        frm_map = ttk.LabelFrame(self, text="Field Mapping (MAIN ↔ KEY)")
        frm_map.grid(row=2, column=0, columnspan=2, sticky="nsew", **pad)

        ttk.Button(frm_map, text="Preview headers", command=self._do_preview, style="FormButton.TButton").grid(row=0, column=0, sticky="w")

        ttk.Label(frm_map, text="Main fields", style="FormLabel.TLabel").grid(row=1, column=0, sticky="w", pady=(6,2))
        ttk.Label(frm_map, text="Key fields", style="FormLabel.TLabel").grid(row=1, column=2, sticky="w", pady=(6,2))

        self.lst_main = tk.Listbox(frm_map, height=10, exportselection=False)
        self.lst_key  = tk.Listbox(frm_map, height=10, exportselection=False)
        self.lst_main.grid(row=2, column=0, sticky="nsew")
        self.lst_key.grid(row=2, column=2, sticky="nsew")

        frm_map.columnconfigure(0, weight=1)
        frm_map.columnconfigure(2, weight=1)
        frm_map.rowconfigure(2, weight=1)

        # Mapping controls
        frm_mid = ttk.Frame(frm_map)
        frm_mid.grid(row=2, column=1, sticky="ns", padx=8)
        ttk.Button(frm_mid, text="Add Pair  ➜", command=self._add_pair, style="FormButton.TButton").grid(row=0, column=0, pady=(12,6))
        ttk.Button(frm_mid, text="Remove Selected", command=self._remove_selected, style="FormButton.TButton").grid(row=1, column=0, pady=6)
        ttk.Button(frm_mid, text="Clear", command=self._clear_pairs, style="FormButton.TButton").grid(row=2, column=0, pady=6)
        ttk.Separator(frm_mid, orient="horizontal").grid(row=3, column=0, sticky="ew", pady=6)
        ttk.Button(frm_mid, text="Auto-map by name", command=self._auto_map_by_name, style="FormButton.TButton").grid(row=4, column=0, pady=(4,12))

        cols = ("main_idx", "main_name", "key_idx", "key_name")
        self.tv_map = ttk.Treeview(frm_map, columns=cols, show="headings", height=8)
        for c, label in zip(cols, ["MAIN Idx", "MAIN Name", "KEY Idx", "KEY Name"]):
            self.tv_map.heading(c, text=label)
            self.tv_map.column(c, width=150, anchor="w")
        self.tv_map.grid(row=3, column=0, columnspan=3, sticky="nsew", pady=(8,0))
        frm_map.rowconfigure(3, weight=1)

        # Output file overrides
        frm_out = ttk.LabelFrame(self, text="Output (optional overrides)")
        frm_out.grid(row=3, column=0, columnspan=2, sticky="nsew", **pad)

        self.var_match_out = tk.StringVar(value="")
        self.var_non_out   = tk.StringVar(value="")
        ttk.Label(frm_out, text="Matched output:", style="FormLabel.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm_out, textvariable=self.var_match_out, width=70).grid(row=0, column=1, sticky="ew", padx=(6,6))
        ttk.Button(frm_out, text="Browse...", command=self._choose_match_out, style="FormButton.TButton").grid(row=0, column=2, sticky="e")

        ttk.Label(frm_out, text="Non-matched output:", style="FormLabel.TLabel").grid(row=1, column=0, sticky="w")
        ttk.Entry(frm_out, textvariable=self.var_non_out, width=70).grid(row=1, column=1, sticky="ew", padx=(6,6))
        ttk.Button(frm_out, text="Browse...", command=self._choose_non_out, style="FormButton.TButton").grid(row=1, column=2, sticky="e")
        frm_out.columnconfigure(1, weight=1)

        # Bottom buttons
        frm_btns = ttk.Frame(self)
        frm_btns.grid(row=4, column=0, columnspan=2, sticky="ew", **pad)
        frm_btns.columnconfigure(0, weight=1)

        ttk.Button(frm_btns, text="Return Config", command=self._on_return_config, style="FormButton.TButton").grid(row=0, column=1, sticky="e", padx=(0,6))
        ttk.Button(frm_btns, text="Run Now", command=self._on_run_now, style="FormButton.TButton").grid(row=0, column=2, sticky="e")
        ttk.Button(frm_btns, text="Cancel", command=self._on_cancel, style="FormButton.TButton").grid(row=0, column=3, sticky="e", padx=(6,0))

        # Resizable grid
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=1)
        for r in range(5):
            self.rowconfigure(r, weight=0)
        self.rowconfigure(2, weight=1)

    def _wire_events(self):
        self.bind("<Escape>", lambda e: self._on_cancel())

    def _center_geometry(self, master) -> str:
        """Center, then shift a bit right; ensure fully on-screen."""
        self.update_idletasks()
        w, h = 1080, 720
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()

        if master:
            mx = master.winfo_rootx()
            my = master.winfo_rooty()
            mw = master.winfo_width()
            mh = master.winfo_height()
            x = mx + (mw - w) // 2
            y = my + (mh - h) // 2
        else:
            x = (sw - w) // 2
            y = (sh - h) // 2

        x += 80  # nudge to the right
        # clamp inside screen with a small margin
        x = max(20, min(x, sw - w - 20))
        y = max(20, min(y, sh - h - 20))
        return f"{w}x{h}+{x}+{y}"

    # ---------- File pickers ----------

    def _choose_main(self):
        path = filedialog.askopenfilename(title="Select MAIN file",
                                          filetypes=[("CSV or text", "*.csv *.txt;*.*"), ("All files", "*.*")])
        if path:
            self.var_main_path.set(path)

    def _choose_key(self):
        path = filedialog.askopenfilename(title="Select KEY file",
                                          filetypes=[("CSV or text", "*.csv *.txt;*.*"), ("All files", "*.*")])
        if path:
            self.var_key_path.set(path)

    def _choose_match_out(self):
        path = filedialog.asksaveasfilename(title="Matched output file",
                                            defaultextension=".csv",
                                            initialfile="MAIN_MATCHED.csv",
                                            filetypes=[("CSV", "*.csv"), ("All files", "*.*")])
        if path:
            self.var_match_out.set(path)

    def _choose_non_out(self):
        path = filedialog.asksaveasfilename(title="Non-matched output file",
                                            defaultextension=".csv",
                                            initialfile="MAIN_NONMATCHED.csv",
                                            filetypes=[("CSV", "*.csv"), ("All files", "*.*")])
        if path:
            self.var_non_out.set(path)

    # ---------- Delimiter helpers ----------

    def _auto_detect_main(self):
        p = Path(self.var_main_path.get())
        if not p.exists():
            messagebox.showwarning("Auto-detect", "Pick a MAIN file first.")
            return
        self.var_main_delim.set(_sniff_delimiter(p))

    def _auto_detect_key(self):
        p = Path(self.var_key_path.get())
        if not p.exists():
            messagebox.showwarning("Auto-detect", "Pick a KEY file first.")
            return
        self.var_key_delim.set(_sniff_delimiter(p))

    def _sync_write_header_default(self):
        self.var_write_header.set(self.var_main_has_header.get())

    # ---------- Preview & mapping ----------

    def _do_preview(self):
        cfg = self._build_minimal_config_for_preview()
        if not cfg:
            return

        # First pass preview
        prev = self._safe_preview(cfg)
        if not prev:
            return

        self.preview_main_header = list(prev.get("main_header") or [])
        self.preview_key_header  = list(prev.get("key_header") or [])

        # Fallback: if we got a single "cell" header, sniff and retry automatically
        if len(self.preview_main_header) <= 1:
            sniffed = _sniff_delimiter(Path(cfg["main_file"]), cfg["main_delimiter"])
            if sniffed != cfg["main_delimiter"]:
                cfg["main_delimiter"] = sniffed
                self.var_main_delim.set(sniffed)
                prev = self._safe_preview(cfg)
                if not prev:
                    return
                self.preview_main_header = list(prev.get("main_header") or [])

        if len(self.preview_key_header) <= 1:
            sniffed = _sniff_delimiter(Path(cfg["key_file"]), cfg["key_delimiter"])
            if sniffed != cfg["key_delimiter"]:
                cfg["key_delimiter"] = sniffed
                self.var_key_delim.set(sniffed)
                prev = self._safe_preview(cfg)
                if not prev:
                    return
                self.preview_key_header = list(prev.get("key_header") or [])

        # If still no headers, synthesize by width
        if not self.preview_main_header:
            w = _peek_width(Path(cfg["main_file"]), cfg["main_delimiter"], cfg["main_has_header"], logger=self.logger)
            self.preview_main_header = [f"Col{idx}" for idx in range(w)]
        if not self.preview_key_header:
            w = _peek_width(Path(cfg["key_file"]), cfg["key_delimiter"], cfg["key_has_header"], logger=self.logger)
            self.preview_key_header = [f"Col{idx}" for idx in range(w)]

        # Populate listboxes (vertical)
        self.lst_main.delete(0, tk.END)
        self.lst_key.delete(0, tk.END)
        for idx, name in enumerate(self.preview_main_header):
            self.lst_main.insert(tk.END, _fmt_field_label(idx, name))
        for idx, name in enumerate(self.preview_key_header):
            self.lst_key.insert(tk.END, _fmt_field_label(idx, name))

        self._clear_pairs()
        self.var_write_header.set(self.var_main_has_header.get())

        if self.logger:
            self.logger.info("[9x UI] Preview loaded. main_header=%d, key_header=%d",
                             len(self.preview_main_header), len(self.preview_key_header))

    def _safe_preview(self, cfg) -> Optional[Dict[str, Any]]:
        try:
            return engine.prepare_preview(cfg, self.logger)
        except Exception as e:
            messagebox.showerror("Preview failed", str(e))
            return None

    def _add_pair(self):
        sm = self.lst_main.curselection()
        sk = self.lst_key.curselection()
        if not sm or not sk:
            messagebox.showwarning("Add Pair", "Select one MAIN field and one KEY field.")
            return
        mi = int(sm[0])
        ki = int(sk[0])
        self.mappings.append((mi, ki))
        self._refresh_map_table()

    def _remove_selected(self):
        sel = self.tv_map.selection()
        if not sel:
            return
        keep: List[Tuple[int, int]] = []
        to_remove = set()
        for item in sel:
            vals = self.tv_map.item(item, "values")
            try:
                to_remove.add((int(vals[0]), int(vals[2])))
            except Exception:
                pass
        for pair in self.mappings:
            if pair not in to_remove:
                keep.append(pair)
        self.mappings = keep
        self._refresh_map_table()

    def _clear_pairs(self):
        self.mappings.clear()
        self._refresh_map_table()

    def _refresh_map_table(self):
        for iid in self.tv_map.get_children():
            self.tv_map.delete(iid)
        for mi, ki in self.mappings:
            mn = self.preview_main_header[mi] if 0 <= mi < len(self.preview_main_header) else f"Col{mi}"
            kn = self.preview_key_header[ki]  if 0 <= ki < len(self.preview_key_header)  else f"Col{ki}"
            self.tv_map.insert("", "end", values=(mi, mn, ki, kn))

    def _auto_map_by_name(self):
        if not self.preview_main_header or not self.preview_key_header:
            messagebox.showinfo("Auto-map", "Run Preview first so we can see field names.")
            return

        def norm(s: str) -> str:
            return (s or "").strip().lower()

        key_index: Dict[str, int] = {}
        for i, nm in enumerate(self.preview_key_header):
            key_index.setdefault(norm(nm), i)

        pairs: List[Tuple[int, int]] = []
        for i, nm in enumerate(self.preview_main_header):
            j = key_index.get(norm(nm))
            if j is not None:
                pairs.append((i, j))

        if not pairs:
            messagebox.showinfo("Auto-map", "No common names found.")
            return

        self.mappings = pairs
        self._refresh_map_table()
        messagebox.showinfo("Auto-map", f"Added {len(pairs)} mapping pair(s) by name.")

    # ---------- Build config ----------

    def _build_minimal_config_for_preview(self) -> Optional[Dict[str, Any]]:
        main = self.var_main_path.get().strip()
        key  = self.var_key_path.get().strip()
        if not main or not key:
            messagebox.showwarning("Missing files", "Pick both MAIN and KEY files.")
            return None
        return {
            "main_file": main,
            "key_file": key,
            "main_delimiter": self.var_main_delim.get() or ",",
            "key_delimiter": self.var_key_delim.get() or ",",
            "main_has_header": bool(self.var_main_has_header.get()),
            "key_has_header": bool(self.var_key_has_header.get()),
        }

    def _build_full_config(self) -> Optional[Dict[str, Any]]:
        base = self._build_minimal_config_for_preview()
        if not base:
            return None

        if not self.mappings:
            messagebox.showwarning("Missing mapping", "Add at least one MAIN ↔ KEY field pair.")
            return None

        manual_pads = _parse_int_list(self.var_manual_pads.get().strip())
        cfg: Dict[str, Any] = dict(base)
        cfg.update({
            "match_fields_main": [mi for (mi, _) in self.mappings],
            "match_fields_key":  [ki for (_,  ki) in self.mappings],
            "case_insensitive": bool(self.var_case_insensitive.get()),
            "strip_whitespace": bool(self.var_strip_ws.get()),
            "auto_zero_pad": bool(self.var_auto_zpad.get()),
            **({"zero_pad_widths": manual_pads} if manual_pads is not None else {}),
            "write_header": bool(self.var_write_header.get()),
        })

        m_out = self.var_match_out.get().strip()
        n_out = self.var_non_out.get().strip()
        if m_out:
            cfg["matched_output_file"] = m_out
        if n_out:
            cfg["nonmatched_output_file"] = n_out

        return cfg

    # ---------- Bottom buttons ----------

    def _on_return_config(self):
        cfg = self._build_full_config()
        if not cfg:
            return
        self._return_mode = "config"
        self._result_config = cfg
        self.destroy()

    def _on_run_now(self):
        cfg = self._build_full_config()
        if not cfg:
            return
        try:
            payload = engine.run(cfg, self.logger, self.shared_data or {})
            self._result_payload = payload
            msg = (
                "Split complete.\n\n"
                f"Rows read (MAIN): {payload.get('rows_read_main',0)}\n"
                f"Rows matched    : {payload.get('rows_matched',0)}\n"
                f"Rows non-matched: {payload.get('rows_nonmatched',0)}\n\n"
                f"Matched file   : {payload.get('matched_output_file','')}\n"
                f"Non-matched    : {payload.get('nonmatched_output_file','')}\n"
                f"Counts report  : {payload.get('counts_report_path','')}\n"
            )
            messagebox.showinfo("Option 9", msg)
        except Exception as e:
            messagebox.showerror("Run failed", str(e))
            return

        self._return_mode = "run"
        self.destroy()

    def _on_cancel(self):
        self._return_mode = "cancel"
        self.destroy()


# ------------------------------
# Public entry points
# ------------------------------

def _ensure_root():
    root = tk._default_root
    if root is None:
        root = tk.Tk()
        root.withdraw()
        _init_theme_and_styles()
    return root

def get_key_split_config_gui(shared_data: Optional[Dict[str, Any]] = None, logger=None) -> Optional[Dict[str, Any]]:
    root = _ensure_root()
    dlg = KeySplitConfigDialog(root, shared_data, logger)
    root.wait_window(dlg)
    if dlg._return_mode == "config":
        return dlg._result_config
    return None

def run_key_split_workflow_gui(shared_data: Dict[str, Any], logger) -> Optional[Dict[str, Any]]:
    root = _ensure_root()
    dlg = KeySplitConfigDialog(root, shared_data, logger)
    root.wait_window(dlg)
    if dlg._return_mode == "run":
        return dlg._result_payload
    return None


# -------------- manual test --------------
if __name__ == "__main__":
    result = run_key_split_workflow_gui(shared_data={}, logger=None)
    print("Result:", result)
