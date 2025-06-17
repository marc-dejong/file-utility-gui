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
from tkinter import simpledialog, ttk

def get_replacement_config_gui(header_fields):
    """
    GUI prompt for replacement config:
    - Select column
    - Enter old and new value
    Returns a config dict with target column and replacements map
    """
    class ReplaceDialog(simpledialog.Dialog):
        def body(self, master):
            self.title("Configure Replacement Rule")

            ttk.Label(master, text="Select Target Column:").grid(row=0, column=0, sticky="w")
            self.column_var = tk.StringVar()
            self.column_combo = ttk.Combobox( master, textvariable=self.column_var, values=header_fields,
                                              state="readonly" )
            self.column_combo.bind( "<<ComboboxSelected>>", lambda e: self.column_var.set( self.column_combo.get() ) )
            print( "[DEBUG] Dropdown binding set" )

            self.column_combo.grid( row=0, column=1, padx=10, pady=5 )

            if header_fields:
                self.column_var.set( header_fields[0] )
                self.column_combo.current( 0 )
                self.column_combo.event_generate( "<<ComboboxSelected>>" )  # ✅ Force selection event

            ttk.Label(master, text="Value to Replace:").grid(row=1, column=0, sticky="w")
            self.old_entry = ttk.Entry( master )
            self.old_entry.grid( row=1, column=1, padx=10, pady=5 )

            ttk.Label(master, text="Replacement Value:").grid(row=2, column=0, sticky="w")
            self.new_entry = ttk.Entry( master )
            self.new_entry.grid( row=2, column=1, padx=10, pady=5 )

            return self.column_combo  # initial focus

        def apply(self):
            selected_field = self.column_var.get().strip()
            old_val = self.old_entry.get().strip()
            new_val = self.new_entry.get().strip()

            print( "DEBUG: apply() called" )
            print( "  Selected Field:", self.column_var.get() )
            print( "  Old Value     :", self.old_entry.get() )
            print( "  New Value     :", self.new_entry.get() )

            if not selected_field or not old_val:
                print( "[ERROR] Field or old value missing." )
                self.result = {}
                return

            self.result = {
                "rules": [
                    {
                        "field": selected_field,
                        "old_value": old_val,
                        "new_value": new_val,
                        "match_type": "exact",  # Default rule
                        "case_sensitive": False  # Could tie this to a checkbox later
                    }
                ]
            }

    root = tk.Tk()
    root.withdraw()  # Hide main window
    dialog = ReplaceDialog( root )
    root.destroy()  # Proper cleanup

    print( "[DEBUG] GUI dialog returning config:", dialog.result )
    return dialog.result or {}


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
    print( "[DEBUG] GUI dialog returning config:", dialog.result )

    return {
        "rules": replacement_rules
    }
