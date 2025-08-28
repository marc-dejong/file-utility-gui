"""
Option 12 — Merge by Key (GUI)

Responsibilities:
  • Collect config for inner/anti join by key
  • Reuse field-mapping GUI to align MAIN and KEY fields
  • Call key_merge_12x.prepare_preview/run and show completion

Integrates with 0x just like Options 6 and 11.
"""
from __future__ import annotations

from typing import Dict, Any, Optional, List
import os
import logging
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

try:
    import key_merge_12x as core
except Exception as e:  # pragma: no cover
    core = None
    _IMPORT_ERROR = e
else:
    _IMPORT_ERROR = None

class NameMapDialog(tk.Toplevel):
    """
    Minimal mapping UI:
      • Left: MAIN columns (names)
      • Right: Selected match fields (ordered)
      • On OK: returns ordered list of MAIN field names
    """
    def __init__(self, parent, main_header: list[str]):
        super().__init__(parent)
        self.title("Map Fields — Pick from MAIN (by name)")
        self.transient(parent)
        self.grab_set()

        self.result = None
        self._main = list(main_header)
        self._chosen: list[str] = []

        frm = ttk.Frame(self); frm.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Available
        left = ttk.Frame(frm); left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(left, text="MAIN columns").pack(anchor="w")
        self.lb_av = tk.Listbox(left, selectmode=tk.EXTENDED, height=14)
        self.lb_av.pack(fill=tk.BOTH, expand=True)
        for name in self._main:
            self.lb_av.insert(tk.END, name)
        ttk.Button(left, text="Add ▶", command=self._add).pack(pady=(6,0), anchor="w")

        # Selected
        right = ttk.Frame(frm); right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10,0))
        ttk.Label(right, text="Selected (match order)").pack(anchor="w")
        self.lb_sel = tk.Listbox(right, selectmode=tk.EXTENDED, height=14)
        self.lb_sel.pack(fill=tk.BOTH, expand=True)
        rbtns = ttk.Frame(right); rbtns.pack(anchor="w", pady=(6,0))
        ttk.Button(rbtns, text="▲ Up", command=self._up).pack(side=tk.LEFT)
        ttk.Button(rbtns, text="▼ Down", command=self._down).pack(side=tk.LEFT, padx=6)
        ttk.Button(rbtns, text="Remove", command=self._remove).pack(side=tk.LEFT, padx=6)
        ttk.Button(rbtns, text="Clear", command=self._clear).pack(side=tk.LEFT)

        # OK/Cancel
        btns = ttk.Frame(self); btns.pack(fill=tk.X, padx=10, pady=(0,10))
        ttk.Button(btns, text="OK", command=self._ok).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side=tk.RIGHT, padx=(0,6))

        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _add(self):
        for i in self.lb_av.curselection():
            name = self.lb_av.get(i)
            if name not in self._chosen:
                self._chosen.append(name)
                self.lb_sel.insert(tk.END, name)

    def _remove(self):
        for i in reversed(self.lb_sel.curselection()):
            name = self.lb_sel.get(i)
            self.lb_sel.delete(i)
            if name in self._chosen:
                self._chosen.remove(name)

    def _clear(self):
        self.lb_sel.delete(0, tk.END)
        self._chosen.clear()

    def _up(self):
        for i in self.lb_sel.curselection():
            if i == 0: continue
            txt = self.lb_sel.get(i)
            self.lb_sel.delete(i); self.lb_sel.insert(i-1, txt)
            self.lb_sel.selection_set(i-1)

    def _down(self):
        for i in reversed(self.lb_sel.curselection()):
            if i >= self.lb_sel.size() - 1: continue
            txt = self.lb_sel.get(i)
            self.lb_sel.delete(i); self.lb_sel.insert(i+1, txt)
            self.lb_sel.selection_set(i+1)

    def _ok(self):
        self.result = [self.lb_sel.get(i) for i in range(self.lb_sel.size())]
        self.grab_release(); self.destroy()

    def _cancel(self):
        self.result = None
        self.grab_release(); self.destroy()


class KeyMergeDialog:
    def __init__(self, root, shared_data: Dict[str, Any]):
        self.root = root
        self.shared_data = shared_data
        self.logger = logging.getLogger("keymerge_ui")
        self.top: Optional[tk.Toplevel] = None

        # Tk vars (seed from shared_data when possible)
        self.var_main_file = tk.StringVar(value=shared_data.get("input_file") or "")
        self.var_main_delim = tk.StringVar(value=shared_data.get("delimiter") or ";")
        self.var_main_has_header = tk.BooleanVar(value=shared_data.get("has_header", True))

        self.var_key_file = tk.StringVar(value=shared_data.get("second_file") or "")
        self.var_key_delim = tk.StringVar(value=shared_data.get("delimiter") or ";")
        self.var_key_has_header = tk.BooleanVar(value=True)

        self.var_join_mode = tk.StringVar(value="inner")  # inner | anti
        self.var_case_ins = tk.BooleanVar(value=True)
        self.var_strip_ws = tk.BooleanVar(value=True)
        self.var_auto_zpad = tk.BooleanVar(value=True)

        self.var_output = tk.StringVar(value=shared_data.get("output_file") or "")
        self.var_write_header = tk.BooleanVar(value=True)

        # Selected field indexes
        self._mf_main: List[int] = []
        self._mf_key:  List[int] = []

    # ---------------- Public API -----------------
    def open(self) -> Optional[Dict[str, Any]]:
        if _IMPORT_ERROR is not None:
            messagebox.showerror("Import error", f"Failed to import key_merge_12x: {_IMPORT_ERROR}")
            return None

        self.top = tk.Toplevel(self.root)
        self.top.title("Option 12 — Merge by Key")
        self.top.transient(self.root)
        self.top.grab_set()
        self.top.protocol("WM_DELETE_WINDOW", self.on_cancel)

        self.logger.info(f"[UI] using core module: {core.__file__}")

        frm_cfg = ttk.LabelFrame(self.top, text="Configuration")
        frm_cfg.pack(fill=tk.X, padx=10, pady=10)

        r = 0
        ttk.Label(frm_cfg, text="MAIN file:").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_main_file, width=58).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Button(frm_cfg, text="Browse…", command=self._browse_main).grid(row=r, column=2, padx=5, pady=4)
        frm_cfg.columnconfigure(1, weight=1)

        r += 1
        ttk.Label(frm_cfg, text="Delimiter:").grid(row=r, column=0, sticky="e", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_main_delim, width=5).grid(row=r, column=1, sticky="w", padx=5, pady=4)
        ttk.Checkbutton(frm_cfg, text="Has header", variable=self.var_main_has_header).grid(row=r, column=2, sticky="w", padx=5, pady=4)

        r += 1
        ttk.Label(frm_cfg, text="KEY file:").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_key_file, width=58).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Button(frm_cfg, text="Browse…", command=self._browse_key).grid(row=r, column=2, padx=5, pady=4)

        r += 1
        ttk.Label(frm_cfg, text="Delimiter:").grid(row=r, column=0, sticky="e", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_key_delim, width=5).grid(row=r, column=1, sticky="w", padx=5, pady=4)
        ttk.Checkbutton(frm_cfg, text="Has header", variable=self.var_key_has_header).grid(row=r, column=2, sticky="w", padx=5, pady=4)

        r += 1
        ttk.Label(frm_cfg, text="Output file:").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_output, width=58).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Button(frm_cfg, text="Save as…", command=self._browse_save).grid(row=r, column=2, padx=5, pady=4)

        # Options
        frm_opts = ttk.LabelFrame(self.top, text="Options")
        frm_opts.pack(fill=tk.X, padx=10, pady=(0,10))
        ttk.Radiobutton(frm_opts, text="Keep matches (inner join)", value="inner", variable=self.var_join_mode).pack(side=tk.LEFT, padx=8)
        ttk.Radiobutton(frm_opts, text="Keep non-matches (anti-join)", value="anti", variable=self.var_join_mode).pack(side=tk.LEFT)
        ttk.Checkbutton(frm_opts, text="Case-insensitive", variable=self.var_case_ins).pack(side=tk.LEFT, padx=12)
        ttk.Checkbutton(frm_opts, text="Strip whitespace", variable=self.var_strip_ws).pack(side=tk.LEFT)
        ttk.Checkbutton(frm_opts, text="Write header", variable=self.var_write_header).pack(side=tk.LEFT, padx=12)
        ttk.Checkbutton(frm_opts, text="Auto zero-pad digits (from KEY)",
                        variable=self.var_auto_zpad).pack(side=tk.LEFT, padx=12)

        # Buttons
        frm_btn = ttk.Frame(self.top)
        frm_btn.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Button(frm_btn, text="Map Fields…", command=self.on_map_fields).pack(side=tk.LEFT)
        ttk.Button(frm_btn, text="Preview", command=self.on_preview).pack(side=tk.LEFT, padx=6)
        self.btn_run = ttk.Button(frm_btn, text="Run", command=self.on_run, state=tk.DISABLED)
        self.btn_run.pack(side=tk.LEFT)
        ttk.Button(frm_btn, text="Cancel", command=self.on_cancel).pack(side=tk.RIGHT)

        self.top.wait_window(self.top)
        return getattr(self, "_result_payload", None)

    # ---------------- Events -----------------
    def on_map_fields(self):
        # Peek headers
        main_hdr = self._peek_header(self.var_main_file.get(), self.var_main_delim.get(),
                                     self.var_main_has_header.get())
        key_hdr = self._peek_header(self.var_key_file.get(), self.var_key_delim.get(), self.var_key_has_header.get())

        if not main_hdr:
            messagebox.showerror("Mapping", "Could not read MAIN header.")
            return
        if not key_hdr:
            messagebox.showerror("Mapping", "Could not read KEY header.")
            return

        # 1) Let user pick the MAIN fields by NAME (order = match order)
        dlg = NameMapDialog(self.top, main_hdr)
        self.top.wait_window(dlg)
        chosen_names = dlg.result
        if not chosen_names:
            messagebox.showwarning("Mapping", "No fields selected.")
            return

        # 2) Map MAIN names -> indices (case-insensitive)
        lookup_main = {h.lower(): i for i, h in enumerate(main_hdr)}
        mf_main = []
        missing_main = []
        for nm in chosen_names:
            idx = lookup_main.get((nm or "").lower())
            if idx is None:
                missing_main.append(nm)
            else:
                mf_main.append(idx)

        # 3) Map the same NAMES onto KEY header (case-insensitive)
        lookup_key = {h.lower(): i for i, h in enumerate(key_hdr)}
        mf_key = []
        missing_key = []
        for nm in chosen_names:
            idx = lookup_key.get((nm or "").lower())
            if idx is None:
                missing_key.append(nm)
            else:
                mf_key.append(idx)

        # 4) Handle missing
        warn_msgs = []
        if missing_main:
            warn_msgs.append(f"Not found in MAIN: {', '.join(missing_main)}")
        if missing_key:
            warn_msgs.append(f"Not found in KEY: {', '.join(missing_key)}")
        if warn_msgs:
            messagebox.showwarning("Mapping", ";\n".join(warn_msgs))
            if missing_key:
                return  # can't proceed if key-side names missing

        # 5) Save & enable Run
        self._mf_main = mf_main
        self._mf_key = mf_key
        self.btn_run.config(state=tk.NORMAL)
        messagebox.showinfo("Mapping Set", f"Mapped {len(self._mf_main)} field(s) by name.")

    def on_preview(self):
        cfg = self._collect_cfg()
        try:
            payload = core.prepare_preview(cfg, self.logger)
        except Exception as e:
            messagebox.showerror("Preview failed", str(e))
            return
        msg = (
            "Preview OK\n\n"
            f"MAIN enc/delim: {payload.get('enc_main')} / {payload.get('main_delimiter','')}\n"
            f"KEY  enc/delim: {payload.get('enc_key')} / {payload.get('key_delimiter','')}\n"
        )
        messagebox.showinfo("Preview", msg)

    def on_run(self):
        if not self._mf_main or not self._mf_key:
            messagebox.showwarning("Run", "Please map fields first.")
            return
        cfg = self._collect_cfg()
        cfg["match_fields_main"] = self._mf_main
        cfg["match_fields_key"]  = self._mf_key
        try:
            result = core.run(cfg, self.logger, self.shared_data)
        except Exception as e:
            messagebox.showerror("Run failed", str(e))
            return
        self._result_payload = result
        self.render_completion(result)
        self.on_cancel()

    def on_cancel(self):
        if self.top is not None:
            try:
                self.top.grab_release()
            except Exception:
                pass
            self.top.destroy()

    # ---------------- Helpers -----------------
    def _browse_main(self):
        f = filedialog.askopenfilename(title="Select MAIN file", filetypes=[("CSV/TXT", ".csv .txt"), ("All files", ".*")])
        if f:
            self.var_main_file.set(f)
            # pre-seed output name
            base, ext = os.path.splitext(f)
            self.var_output.set(base + "_RESULTS.csv")

    def _browse_key(self):
        f = filedialog.askopenfilename(title="Select KEY file", filetypes=[("CSV/TXT", ".csv .txt"), ("All files", ".*")])
        if f:
            self.var_key_file.set(f)

    def _browse_save(self):
        f = filedialog.asksaveasfilename(defaultextension=".csv", title="Save results as…",
                                         filetypes=[("CSV", ".csv"), ("All files", ".*")])
        if f:
            self.var_output.set(f)

    def _peek_header(self, path, delim, has_header):
        import csv
        if not path:
            return []
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                r = csv.reader(f, delimiter=delim or ",")
                row = next(r, [])
            return row if has_header else [f"Field {i+1}" for i in range(len(row))]
        except Exception:
            return []

    def _collect_cfg(self) -> Dict[str, Any]:
        cfg = {
            "main_file": self.var_main_file.get().strip(),
            "main_delimiter": (self.var_main_delim.get() or ","),
            "main_has_header": bool(self.var_main_has_header.get()),
            "key_file": self.var_key_file.get().strip(),
            "key_delimiter": (self.var_key_delim.get() or ","),
            "key_has_header": bool(self.var_key_has_header.get()),
            "join_mode": (self.var_join_mode.get() or "inner"),
            "case_insensitive": bool(self.var_case_ins.get()),
            "strip_whitespace": bool(self.var_strip_ws.get()),
            "output_file": self.var_output.get().strip(),
            "write_header": bool(self.var_write_header.get()),
            "auto_zero_pad": bool(self.var_auto_zpad.get()),
        }
        return cfg

    def render_completion(self, result_payload):
        # Get mapped field indexes FIRST
        mf = result_payload.get("match_fields_main", []) or []
        kf = result_payload.get("match_fields_key", []) or []

        # Peek headers to show names alongside indexes
        main_hdr = self._peek_header(self.var_main_file.get(),
                                     self.var_main_delim.get(), True)
        key_hdr = self._peek_header(self.var_key_file.get(),
                                    self.var_key_delim.get(), True)

        pairs = []
        for i, (a, b) in enumerate(zip(mf, kf), start=1):
            an = main_hdr[a] if 0 <= a < len(main_hdr) else f"MAIN[{a}]"
            bn = key_hdr[b] if 0 <= b < len(key_hdr) else f"KEY[{b}]"
            pairs.append(f"  {i}) MAIN[{a}] {an!r} ↔ KEY[{b}] {bn!r}")
        pairs = "\n".join(pairs)

        out = result_payload.get("output_file", "")
        counts = result_payload.get("counts_report_path", "")
        rows = result_payload.get("records_written", 0)
        mode = result_payload.get("join_mode", "inner")

        msg = (
            "Option 12 — Merge by Key Completed\n\n"
            f"Output file: {out}\n"
            f"Counts report: {counts}\n\n"
            f"Join mode: {mode}\n"
            f"Rows written: {rows}\n\n"
            f"Match fields:\n{pairs}"
        )
        try:
            from tkinter import messagebox
            messagebox.showinfo("Merge by Key — Completed", msg, parent=self.top)
        except Exception:
            pass


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    root = tk.Tk(); root.withdraw()
    shared = {}
    dlg = KeyMergeDialog(root, shared)
    dlg.open()
