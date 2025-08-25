"""
4x_Replacer_UI.py
-----------------
Handles interactive user input for content replacement configuration.

Prompts the user to:
- Select target fields (from header) for substitution
- Define old → new value pairs
- Choose match type (exact or substring)
- Specify case sensitivity

Returns a structured configuration dictionary to be used by 4x_Replacer.py.
"""
import tkinter as tk
from tkinter import simpledialog, ttk, messagebox
import logging
logger = logging.getLogger()

def get_replacement_config_gui(header_fields, parent=None):

    """
    Open a modal GUI to collect one replacement rule and return:
      {"rules": [{"field", "old_value", "new_value", "match_type", "case_sensitive", "strip_quotes"}]}
    IMPORTANT: No nested mainloop; use a Toplevel + wait_window.
    """
    import tkinter as tk
    from tkinter import ttk, messagebox

    created_temp_root = False

    # Ensure we have a *valid* parent (winfo_exists guards against destroyed roots)
    if parent is None:
        parent = tk.Tk()
        parent.withdraw()
        created_temp_root = True
    else:
        try:
            if not parent.winfo_exists():
                parent = tk.Tk()
                parent.withdraw()
                created_temp_root = True
        except tk.TclError:
            parent = tk.Tk()
            parent.withdraw()
            created_temp_root = True

    # --- modal dialog ---
    dialog = tk.Toplevel(parent)
    dialog.title("Configure Replacement Rule")
    dialog.transient(parent)
    dialog.resizable(False, False)
    dialog.grab_set()
    try:
        dialog.lift()
        dialog.focus_force()
    except Exception:
        pass

    # --- UI state ---
    field_var = tk.StringVar(dialog, value=(header_fields[0] if header_fields else ""))
    find_value = tk.StringVar(dialog, value="")
    repl_value = tk.StringVar(dialog, value="")
    match_whole_var = tk.IntVar(dialog, value=1)     # checked = exact
    case_sensitive_var = tk.IntVar(dialog, value=0)  # unchecked = case-insensitive
    strip_quotes_var = tk.IntVar(dialog, value=0)

    # --- layout ---
    tk.Label(dialog, text="Select Field:").pack(pady=(10, 0))
    tk.OptionMenu(dialog, field_var, *header_fields).pack()

    tk.Label(dialog, text="Find Value:").pack(pady=(10, 0))
    tk.Entry(dialog, textvariable=find_value, width=40).pack()

    tk.Label(dialog, text="Replace With:").pack(pady=(10, 0))
    tk.Entry(dialog, textvariable=repl_value, width=40).pack()

    tk.Checkbutton(dialog, text="Match whole field only (uncheck for substring match)", variable=match_whole_var).pack(pady=(10, 0))
    tk.Checkbutton(dialog, text="Case-sensitive match", variable=case_sensitive_var).pack()
    tk.Checkbutton(dialog, text="Strip double quotes before comparing", variable=strip_quotes_var).pack()

    result = {}

    def on_submit():
        if not field_var.get() or not find_value.get().strip():
            messagebox.showerror("Missing Information", "Field and Find value are required.", parent=dialog)
            return
        rule = {
            "field": field_var.get(),
            "old_value": find_value.get().strip(),
            "new_value": repl_value.get().strip(),
            # Engine expects "exact" or "substring"
            "match_type": "exact" if match_whole_var.get() else "substring",
            "case_sensitive": bool(case_sensitive_var.get()),
            "strip_quotes": bool(strip_quotes_var.get()),
        }
        result["rules"] = [rule]
        logger.debug("[REPLACER_UI] Collected rule: %s", rule)
        dialog.destroy()

    def on_cancel():
        result.clear()
        dialog.destroy()

    # Buttons
    btns = ttk.Frame(dialog)
    btns.pack(side="bottom", fill="x", padx=8, pady=8)
    ttk.Button(btns, text="OK", command=on_submit).pack(side="right", padx=6)
    ttk.Button(btns, text="Cancel", command=on_cancel).pack(side="right")

    dialog.protocol("WM_DELETE_WINDOW", on_cancel)

    # Gentle center near parent
    dialog.update_idletasks()
    try:
        x = parent.winfo_rootx() + 60
        y = parent.winfo_rooty() + 60
        dialog.geometry(f"+{x}+{y}")
    except Exception:
        pass

    # Block modally (NO second mainloop)
    dialog.wait_window()

    if created_temp_root:
        parent.destroy()

    return result if result else None


def get_replacement_config(header_fields):
    """
    Interactive prompt to gather replacement rules from the user.
    Returns a config dictionary to be used by 4x_Replacer.py
    """
    print( "\n[REPLACER CONFIGURATION]" )
    print( "You will define one or more replacement rules." )
    print( "Each rule will apply to a specific field/column." )

    replacement_rules = []

    while True:
        print( "\nAvailable fields:" )
        for idx, field in enumerate( header_fields ):
            print( f"{idx + 1}. {field}" )

        try:
            field_idx = int( input( "Select the field number to apply a replacement on: " ) ) - 1
            if field_idx < 0 or field_idx >= len( header_fields ):
                print( "[ERROR] Invalid selection. Try again." )
                continue
        except ValueError:
            print( "[ERROR] Enter a number." )
            continue

        target_field = header_fields[field_idx]
        old_value = input( f"Enter the value to search for in '{target_field}': " ).strip()
        new_value = input( f"Enter the replacement value for '{old_value}': " ).strip()

        match_type = input( "Match type? (E = Exact match, S = Substring match) [E/S]: " ).strip().upper()
        case_sensitive = input( "Case sensitive? (Y = Yes, N = No) [Y/N]: " ).strip().upper()

        rule = {
            "field": target_field,
            "old_value": old_value,
            "new_value": new_value,
            "match_type": "exact" if match_type != "S" else "substring",
            "case_sensitive": (case_sensitive == "Y")
        }

        replacement_rules.append( rule )
        print( f"[ADDED RULE] {rule}" )

        more = input( "Add another rule? (Y/N): " ).strip().upper()
        if more != "Y":
            break

    print( f"\n[INFO] Total rules configured: {len( replacement_rules )}" )
    print("[DEBUG] Replacement rules collected:", replacement_rules)

    return {
        "rules": replacement_rules  # ✅ must be present
    }

