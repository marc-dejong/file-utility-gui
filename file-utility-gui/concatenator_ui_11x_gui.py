"""
concatenator_ui_11x_gui.py — Option 11 (Concatenate Files) UI

Modal dialog to collect configuration, run Preview (non-destructive), and trigger
execution. Writes config to `shared_data["concat"]` and calls the core
module functions directly:
  • concatenator_11x.prepare_preview(config, logger)
  • concatenator_11x.run(config, logger, shared_data)

No sys.exit; returns structured payloads and renders a control report summary.
"""
from __future__ import annotations

from typing import Dict, Any, Optional, List
import os
import logging
import tkinter as tk
import re, glob
from tkinter import ttk, filedialog, messagebox

# Local import of the core module
try:
    import concatenator_11x as core
except Exception as e:  # pragma: no cover
    core = None
    _IMPORT_ERROR = e
else:
    _IMPORT_ERROR = None


def parse_extensions(ext_str: str) -> list[str]:
    """
    Accepts comma/semicolon/space/pipe separated patterns, normalizes tokens like
    'csv' or '.csv' to '*.csv', deduplicates, and returns a list of glob patterns.
    """
    if not ext_str or ext_str.strip() in ("*", "*.*"):
        return ["*"]
    tokens = re.split(r"[,\s;|]+", ext_str.strip())
    patterns = []
    for t in tokens:
        if not t:
            continue
        t = t.strip()
        # Normalize: csv -> *.csv, .csv -> *.csv; keep '*.csv' as-is
        if t.startswith("*."):
            patterns.append(t.lower())
        else:
            if t.startswith("."):
                t = f"*{t}"
            elif not t.startswith("*"):
                t = f"*.{t}"
            patterns.append(t.lower())
    # Dedupe, preserve order
    return list(dict.fromkeys(patterns))


class ConcatenateFilesDialog:
    """Modal dialog for Option 11 (Concatenate Files).

    Responsibilities:
      • Collect config values into `shared_data["concat"]`.
      • Call `core.prepare_preview(...)` and render the preview table.
      • Enable Run only after a successful preview; trigger execution.
      • Show completion summary with artifact paths and control equation.
    """

    def __init__(self, root, shared_data: Dict[str, Any]):
        self.root = root
        self.shared_data = shared_data
        self.logger = logging.getLogger("concatenator_ui")
        self.top: Optional[tk.Toplevel] = None
        self._completion_shown = False

        # Tk Variables
        self.var_input_dir = tk.StringVar(value=self.shared_data.get("last_input_dir") or os.getcwd())
        self.var_extensions = tk.StringVar(value="*.csv;*.txt")
        self.var_has_headers = tk.BooleanVar(value=True)
        self.var_delimiter = tk.StringVar(value=";")
        self.var_header_validation = tk.StringVar(value="strict")
        self.var_on_mismatch = tk.StringVar(value="skip")
        self.var_natural_sort = tk.BooleanVar(value=True)
        self.var_skip_empty = tk.BooleanVar(value=True)
        self.var_ignore_hidden = tk.BooleanVar(value=True)
        self.var_output_path = tk.StringVar(value="")

        self.logger.info(f"[UI] using core module: {core.__file__}")

        # UI State
        self._preview_payload: Optional[Dict[str, Any]] = None
        self._result_payload: Optional[Dict[str, Any]] = None

    # ---------------- Public API -----------------
    def open(self) -> Optional[Dict[str, Any]]:
        if _IMPORT_ERROR is not None:
            messagebox.showerror("Import error", f"Failed to import concatenator_11x: {_IMPORT_ERROR}")
            return None

        self.top = tk.Toplevel(self.root)
        self.top.title("Option 11 — Concatenate Files")
        self.top.transient(self.root)
        self.top.grab_set()  # modal
        self.top.protocol("WM_DELETE_WINDOW", self.on_cancel)

        # Layout
        frm_cfg = ttk.LabelFrame(self.top, text="Configuration")
        frm_cfg.pack(fill=tk.X, padx=10, pady=10)

        # Row 0: input dir + browse
        r = 0
        ttk.Label(frm_cfg, text="Input folder:").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_input_dir, width=60).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Button(frm_cfg, text="Browse…", command=self._browse_dir).grid(row=r, column=2, padx=5, pady=4)
        frm_cfg.columnconfigure(1, weight=1)

        # Row 1: extensions, delimiter
        r += 1
        ttk.Label(frm_cfg, text="Extensions (;‑sep):").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_extensions, width=40).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Label(frm_cfg, text="Delimiter:").grid(row=r, column=2, sticky="e", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_delimiter, width=4).grid(row=r, column=3, sticky="w", padx=5, pady=4)

        # Row 2: header options
        r += 1
        ttk.Checkbutton(frm_cfg, text="Has headers", variable=self.var_has_headers).grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Label(frm_cfg, text="Header validation:").grid(row=r, column=1, sticky="e", padx=5, pady=4)
        cmb_val = ttk.Combobox(frm_cfg, textvariable=self.var_header_validation, state="readonly",
                               values=["strict", "compatible", "none"], width=14)
        cmb_val.grid(row=r, column=2, sticky="w", padx=5, pady=4)
        ttk.Label(frm_cfg, text="On mismatch:").grid(row=r, column=3, sticky="e", padx=5, pady=4)
        cmb_mis = ttk.Combobox(frm_cfg, textvariable=self.var_on_mismatch, state="readonly",
                               values=["skip", "abort"], width=10)
        cmb_mis.grid(row=r, column=4, sticky="w", padx=5, pady=4)

        # Row 3: toggles
        r += 1
        ttk.Checkbutton(frm_cfg, text="Natural sort", variable=self.var_natural_sort).grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Checkbutton(frm_cfg, text="Skip empty files", variable=self.var_skip_empty).grid(row=r, column=1, sticky="w", padx=5, pady=4)
        ttk.Checkbutton(frm_cfg, text="Ignore hidden/system", variable=self.var_ignore_hidden).grid(row=r, column=2, sticky="w", padx=5, pady=4)

        # Row 4: output path (optional)
        r += 1
        ttk.Label(frm_cfg, text="Output file (optional):").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_output_path, width=60).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Button(frm_cfg, text="Save as…", command=self._browse_save).grid(row=r, column=2, padx=5, pady=4)

        # Buttons
        frm_btn = ttk.Frame(self.top)
        frm_btn.pack(fill=tk.X, padx=10, pady=(0, 10))
        self.btn_preview = ttk.Button(frm_btn, text="Preview", command=self.on_preview)
        self.btn_run = ttk.Button(frm_btn, text="Run", command=self.on_run, state=tk.DISABLED)
        self.btn_cancel = ttk.Button(frm_btn, text="Cancel", command=self.on_cancel)
        self.btn_preview.pack(side=tk.LEFT)
        self.btn_run.pack(side=tk.LEFT, padx=6)
        self.btn_cancel.pack(side=tk.RIGHT)

        # Preview area
        self.frm_preview = ttk.LabelFrame(self.top, text="Preview")
        self.frm_preview.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self._build_preview_widgets()

        self.top.wait_window(self.top)
        return self._result_payload

    # ---------------- Event handlers -----------------
    def on_preview(self) -> None:
        cfg = self._collect_config()
        self.shared_data["concat"] = cfg
        try:
            payload = core.prepare_preview(cfg, self.logger)
        except Exception as e:
            messagebox.showerror("Preview failed", str(e))
            self.logger.exception("Preview failed")
            return
        self._preview_payload = payload
        self._populate_preview(payload)
        self.btn_run.config(state=tk.NORMAL)

    def on_run(self) -> Optional[Dict[str, Any]]:
        if not self._preview_payload:
            messagebox.showwarning("Run", "Please run Preview first.")
            return None
        cfg = self.shared_data.get("concat", {})
        try:
            result = core.run(cfg, self.logger, self.shared_data)
        except Exception as e:
            messagebox.showerror("Run failed", str(e))
            self.logger.exception("Run failed")
            return None

        self._result_payload = result
        self.shared_data["last_function_ok"] = True

        if not self._completion_shown:
            self._completion_shown = True
            self.render_completion(result)

        self.on_cancel()
        return result

    def on_cancel(self) -> None:
        if self.top is not None:
            self.top.grab_release()
            self.top.destroy()

    # ---------------- UI helpers -----------------
    def _browse_dir(self) -> None:
        d = filedialog.askdirectory(initialdir=self.var_input_dir.get() or os.getcwd(), title="Select input folder")
        if d:
            self.var_input_dir.set(d)
            self.shared_data["last_input_dir"] = d

    def _browse_save(self) -> None:
        f = filedialog.asksaveasfilename(defaultextension=".csv", title="Save results as…",
                                         filetypes=[("CSV", ".csv"), ("All files", ".*")])
        if f:
            self.var_output_path.set(f)

    def _collect_config(self) -> Dict[str, Any]:
        exts = parse_extensions(self.var_extensions.get())

        cfg = {
            "input_dir": self.var_input_dir.get().strip(),
            "extensions": exts,
            "has_headers": bool(self.var_has_headers.get()),
            "delimiter": (self.var_delimiter.get() or ","),
            "header_validation": self.var_header_validation.get(),
            "on_mismatch": self.var_on_mismatch.get(),
            "natural_sort": bool(self.var_natural_sort.get()),
            "skip_empty_files": bool(self.var_skip_empty.get()),
            "ignore_hidden_system": bool(self.var_ignore_hidden.get()),
        }
        out = self.var_output_path.get().strip()
        if out:
            cfg["output_path"] = out
        return cfg

    def _build_preview_widgets(self) -> None:
        # Reference header + stats
        self.lbl_ref = ttk.Label(self.frm_preview, text="Reference header: <none>")
        self.lbl_ref.pack(anchor="w", padx=6, pady=4)

        # Notebook with 2 tabs: include & skip
        self.nb = ttk.Notebook(self.frm_preview)
        self.tab_include = ttk.Frame(self.nb)
        self.tab_skip = ttk.Frame(self.nb)
        self.nb.add(self.tab_include, text="Include")
        self.nb.add(self.tab_skip, text="Skip")
        self.nb.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        # Include tree
        cols_inc = ("filename", "rows",)
        self.tree_inc = ttk.Treeview(self.tab_include, columns=cols_inc, show="headings", height=8)
        self.tree_inc.heading("filename", text="Filename")
        self.tree_inc.heading("rows", text="(est.) rows")
        self.tree_inc.column("filename", width=380, anchor="w")
        self.tree_inc.column("rows", width=100, anchor="e")
        self.tree_inc.pack(fill=tk.BOTH, expand=True)

        # Skip tree
        cols_skip = ("filename", "reason")
        self.tree_skip = ttk.Treeview(self.tab_skip, columns=cols_skip, show="headings", height=8)
        self.tree_skip.heading("filename", text="Filename")
        self.tree_skip.heading("reason", text="Reason")
        self.tree_skip.column("filename", width=380, anchor="w")
        self.tree_skip.column("reason", width=160, anchor="w")
        self.tree_skip.pack(fill=tk.BOTH, expand=True)

    def _populate_preview(self, payload: Dict[str, Any]) -> None:
        # Reference header label
        ref = payload.get("reference_header") or {}
        tokens = ref.get("tokens") or []
        if tokens:
            hdr_txt = ", ".join(tokens)
            if len(hdr_txt) > 120:
                hdr_txt = hdr_txt[:117] + "…"
            self.lbl_ref.config(text=f"Reference header: [{len(tokens)} cols] {hdr_txt}")
        else:
            self.lbl_ref.config(text="Reference header: <none>")

        # Clear trees
        for t in (self.tree_inc, self.tree_skip):
            for i in t.get_children():
                t.delete(i)

        # Include
        for f in payload.get("include_files", []) or []:
            self.tree_inc.insert("", tk.END, values=(f, "–"))
        # Skip
        for item in payload.get("skip_files", []) or []:
            self.tree_skip.insert("", tk.END, values=(item.get("filename"), item.get("reason")))

    def render_completion(self, result_payload: Dict[str, Any]) -> None:
        # Dialog showing control equation and artifact paths
        out = result_payload.get("output_path", "")
        dropped = result_payload.get("dropped_records_path", "")
        counts = result_payload.get("counts_report_path", "")
        total = result_payload.get("total_rows_written", 0)
        dropped_n = result_payload.get("dropped_total", 0)

        # Build control equation text
        left = sum(int(x.get("rows_appended", 0)) for x in (result_payload.get("files_included") or []))
        ctrl_line = f"CONTROL: sum(input rows appended) - dropped = written  ==>  {left} - {dropped_n} = {total}"

        msg = (
            "Option 11 — Concatenate Files\n\n"
            f"Output file: {out}\n"
            f"Dropped records: {dropped}\n"
            f"Counts report: {counts}\n\n"
            f"{ctrl_line}\n"
        )
        messagebox.showinfo("Concatenate — Completed", msg)


# Convenience manual test harness (optional)
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    root = tk.Tk()
    root.withdraw()
    sd = {}
    dlg = ConcatenateFilesDialog(root, sd)
    dlg.open()
