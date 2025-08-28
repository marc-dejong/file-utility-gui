
"""
scissors_ui_6x_gui.py -
Option 6 — Scissors (Column Chooser) — GUI Dialog

Responsibilities:
  • Collect config values into shared_data["scissors"].
  • Preview: reads header to populate column list for selection.
  • Run: triggers core.run(...) and shows a completion summary.

No sys.exit; returns structured payloads and renders a control report summary.
"""
from __future__ import annotations
from typing import Dict, Any, Optional, List
import os
import logging
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# Local import of the core module
try:
    import scissors_6x as core
except Exception as e:  # pragma: no cover
    core = None
    _IMPORT_ERROR = e
else:
    _IMPORT_ERROR = None


class ScissorsDialog:
    def __init__(self, root, shared_data: Dict[str, Any]):
        self.root = root
        self.shared_data = shared_data
        self.logger = logging.getLogger("scissors_ui")
        self.top: Optional[tk.Toplevel] = None
        self._completion_shown = False

        # Tk Vars
        self.var_input_path = tk.StringVar(value=self.shared_data.get("last_input_file") or "")
        self.var_delimiter = tk.StringVar(value=";")
        self.var_has_headers = tk.BooleanVar(value=True)
        self.var_case_insensitive = tk.BooleanVar(value=True)
        self.var_output_path = tk.StringVar(value="")

        # Column selection state
        self.columns: List[str] = []          # available tokens (display names)
        self.filtered_columns: List[str] = []  # filtered view
        self.selected: List[str] = []         # chosen (ordered)

        # For filtering/search
        self.var_search = tk.StringVar(value="")

    # ---------------- Public API -----------------
    def open(self) -> Optional[Dict[str, Any]]:
        if _IMPORT_ERROR is not None:
            messagebox.showerror("Import error", f"Failed to import scissors_6x: {_IMPORT_ERROR}")
            return None

        self.top = tk.Toplevel(self.root)
        self.top.title("Option 6 — Scissors (Column Chooser)")
        self.top.transient(self.root)
        self.top.grab_set()
        self.top.protocol("WM_DELETE_WINDOW", self.on_cancel)

        self.logger.info(f"[UI] using core module: {core.__file__}")

        # Layout
        frm_cfg = ttk.LabelFrame(self.top, text="Configuration")
        frm_cfg.pack(fill=tk.X, padx=10, pady=10)

        r = 0
        ttk.Label(frm_cfg, text="Input file:").grid(row=r, column=0, sticky="w", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_input_path, width=60).grid(row=r, column=1, sticky="we", padx=5, pady=4)
        ttk.Button(frm_cfg, text="Browse…", command=self._browse_input).grid(row=r, column=2, padx=5, pady=4)
        frm_cfg.columnconfigure(1, weight=1)

        r += 1
        ttk.Label(frm_cfg, text="Delimiter:").grid(row=r, column=0, sticky="e", padx=5, pady=4)
        ttk.Entry(frm_cfg, textvariable=self.var_delimiter, width=4).grid(row=r, column=1, sticky="w", padx=5, pady=4)
        ttk.Checkbutton(frm_cfg, text="Has headers", variable=self.var_has_headers).grid(row=r, column=2, sticky="w", padx=5, pady=4)
        ttk.Checkbutton(frm_cfg, text="Case-insensitive match", variable=self.var_case_insensitive).grid(row=r, column=3, sticky="w", padx=5, pady=4)

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

        # Selection UI
        self.frm_sel = ttk.LabelFrame(self.top, text="Select columns to keep (order = output order)")
        self.frm_sel.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Search box
        frm_search = ttk.Frame(self.frm_sel)
        frm_search.pack(fill=tk.X, padx=6, pady=(6, 0))
        ttk.Label(frm_search, text="Search:").pack(side=tk.LEFT)
        ent_search = ttk.Entry(frm_search, textvariable=self.var_search, width=30)
        ent_search.pack(side=tk.LEFT, padx=6)
        ttk.Button(frm_search, text="Filter", command=self._apply_filter).pack(side=tk.LEFT)
        ttk.Button(frm_search, text="Clear", command=self._clear_filter).pack(side=tk.LEFT, padx=(6,0))

        body = ttk.Frame(self.frm_sel)
        body.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        # Available list
        left = ttk.Frame(body)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(left, text="Available").pack(anchor="w")
        self.lb_available = tk.Listbox(left, selectmode=tk.EXTENDED, height=12)
        self.lb_available.pack(fill=tk.BOTH, expand=True)
        ttk.Button(left, text="Select All", command=self._select_all_available).pack(anchor="w", pady=(6,0))

        # Middle buttons
        mid = ttk.Frame(body)
        mid.pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(mid, text=">>", command=self._add_selected).pack(pady=4)
        ttk.Button(mid, text="<<", command=self._remove_selected).pack(pady=4)

        # Selected list
        right = ttk.Frame(body)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(right, text="Selected (output order)").pack(anchor="w")
        self.lb_chosen = tk.Listbox(right, selectmode=tk.EXTENDED, height=12)
        self.lb_chosen.pack(fill=tk.BOTH, expand=True)
        rbtns = ttk.Frame(right)
        rbtns.pack(anchor="w", pady=(6,0))
        ttk.Button(rbtns, text="Up", command=self._move_up).pack(side=tk.LEFT)
        ttk.Button(rbtns, text="Down", command=self._move_down).pack(side=tk.LEFT, padx=6)
        ttk.Button(rbtns, text="Clear", command=self._clear_selected).pack(side=tk.LEFT)

        self.top.wait_window(self.top)
        return getattr(self, "_result_payload", None)

    # ---------------- Event handlers -----------------
    def on_preview(self) -> None:
        cfg = self._collect_config()
        self.shared_data["scissors"] = cfg
        try:
            payload = core.prepare_preview(cfg, self.logger)
        except Exception as e:
            messagebox.showerror("Preview failed", str(e))
            self.logger.exception("Preview failed")
            return
        # Populate available list
        self.columns = list(payload.get("header") or [])
        self._refresh_available(self.columns)
        self.lb_chosen.delete(0, tk.END)
        self.selected = []
        self.btn_run.config(state=tk.NORMAL)

    def render_completion(self, result_payload):
        # Build a simple completion summary (matches your Option 11 style)
        out = result_payload.get("output_path", "")
        counts = result_payload.get("counts_report_path", "")
        rows = result_payload.get("rows_written", 0)
        names = result_payload.get("selected_columns") or []
        idxs = result_payload.get("selected_indices") or []

        if names:
            sel = "\n".join(f"  {i + 1}) {name}" for i, name in enumerate(names))
            sel_hdr = "Selected columns:"
        elif idxs:
            sel = "\n".join(f"  {i + 1}) index {idx}" for i, idx in enumerate(idxs))
            sel_hdr = "Selected columns (by index):"
        else:
            sel = "  <none>"
            sel_hdr = "Selected columns:"

        msg = (
            "Option 6 — Scissors Completed\n\n"
            f"Output file: {out}\n"
            f"Counts report: {counts}\n\n"
            f"Rows written: {rows}\n\n"
            f"{sel_hdr}\n{sel}"
        )
        try:
            from tkinter import messagebox
            messagebox.showinfo("Scissors — Completed", msg, parent=self.top)
        except Exception:
            pass

    def on_run(self) -> Optional[Dict[str, Any]]:
        if not self.columns:
            messagebox.showwarning("Run", "Please run Preview first.")
            return None
        cfg = self.shared_data.get("scissors", {})
        # Capture chosen order into config
        chosen = [self.lb_chosen.get(i) for i in range(self.lb_chosen.size())]
        if self.var_has_headers.get():
            cfg["selected_columns"] = chosen
            cfg.pop("selected_indices", None)
        else:
            # No headers: interpret names like "Col N" as 1-based index
            try:
                cfg["selected_indices"] = [int(name.split()[-1]) - 1 for name in chosen]
            except Exception:
                messagebox.showerror("Run", "In no-header mode, selections must be column positions.")
                return None
            cfg.pop("selected_columns", None)
        self.shared_data["scissors"] = cfg

        try:
            result = core.run(cfg, self.logger, self.shared_data)
        except Exception as e:
            messagebox.showerror("Run failed", str(e))
            self.logger.exception("Run failed")
            return None

        self._result_payload = result
        self.shared_data["last_function_ok"] = True
        self.render_completion(result)
        self.on_cancel()
        return result

    def on_cancel(self) -> None:
        if self.top is not None:
            self.top.grab_release()
            self.top.destroy()

    # ---------------- UI helpers -----------------
    def _browse_input(self) -> None:
        f = filedialog.askopenfilename(title="Select input file", filetypes=[("CSV/TXT", ".csv .txt"), ("All files", ".*")])
        if f:
            self.var_input_path.set(f)
            self.shared_data["last_input_file"] = f

    def _browse_save(self) -> None:
        f = filedialog.asksaveasfilename(defaultextension=".csv", title="Save results as…",
                                         filetypes=[("CSV", ".csv"), ("All files", ".*")])
        if f:
            self.var_output_path.set(f)

    def _collect_config(self) -> Dict[str, Any]:
        cfg = {
            "input_path": self.var_input_path.get().strip(),
            "delimiter": (self.var_delimiter.get() or ","),
            "has_headers": bool(self.var_has_headers.get()),
            "case_insensitive": bool(self.var_case_insensitive.get()),
        }
        out = self.var_output_path.get().strip()
        if out:
            cfg["output_path"] = out
        return cfg

    def _refresh_available(self, tokens: List[str]) -> None:
        self.lb_available.delete(0, tk.END)
        for t in tokens:
            self.lb_available.insert(tk.END, t)
        self.filtered_columns = list(tokens)

    def _apply_filter(self) -> None:
        q = (self.var_search.get() or "").strip().lower()
        if not q:
            self._refresh_available(self.columns)
            return
        toks = [t for t in self.columns if q in t.lower()]
        self._refresh_available(toks)

    def _clear_filter(self) -> None:
        self.var_search.set("")
        self._refresh_available(self.columns)

    def _select_all_available(self) -> None:
        self.lb_available.select_set(0, tk.END)
        self._add_selected()

    def _add_selected(self) -> None:
        sel_idx = list(self.lb_available.curselection())
        for i in sel_idx:
            name = self.lb_available.get(i)
            if name not in self.selected:
                self.selected.append(name)
                self.lb_chosen.insert(tk.END, name)

    def _remove_selected(self) -> None:
        sel_idx = list(self.lb_chosen.curselection())
        # Remove from bottom to top to keep indices stable
        for i in reversed(sel_idx):
            name = self.lb_chosen.get(i)
            self.lb_chosen.delete(i)
            if name in self.selected:
                self.selected.remove(name)

    def _move_up(self) -> None:
        sel = list(self.lb_chosen.curselection())
        for i in sel:
            if i == 0:
                continue
            text = self.lb_chosen.get(i)
            self.lb_chosen.delete(i)
            self.lb_chosen.insert(i-1, text)
            self.lb_chosen.selection_set(i-1)

    def _move_down(self) -> None:
        sel = list(self.lb_chosen.curselection())
        # Work from bottom so indices stay correct when moving down
        for i in reversed(sel):
            if i >= self.lb_chosen.size() - 1:
                continue
            text = self.lb_chosen.get(i)
            self.lb_chosen.delete(i)
            self.lb_chosen.insert(i+1, text)
            self.lb_chosen.selection_set(i+1)

    def _clear_selected(self) -> None:
        self.lb_chosen.delete(0, tk.END)
        self.selected = []


# Convenience manual test harness (optional)
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    root = tk.Tk()
    root.withdraw()
    sd = {}
    dlg = ScissorsDialog(root, sd)
    dlg.open()
