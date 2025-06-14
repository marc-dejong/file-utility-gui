"""
MODULE: 3x_Filters_UI.py

DESCRIPTION:
    This module provides a user interface to collect filtering rules
    for the Multi-Function File Utility system. It supports:
    - Standard string filters (equals, contains, starts_with)
    - Date-based filters using flexible input and format parsing

    Filters collected here are returned as a list of dictionaries
    and are intended to be passed into 3x_Filters.apply_filters().

REQUIRES:
    - prompt_for_date_filter_config() and validate_and_parse_date_config() from utils_11x.py.py
"""
from utils_11x_gui import prompt_for_date_filter_config, validate_and_parse_date_config


import tkinter as tk
from tkinter import simpledialog, ttk

def get_filter_rules_gui(header_fields):
    """
    Launches GUI dialog in a loop to collect multiple filter rules.
    Returns a list of rule dictionaries.
    """
    import tkinter as tk
    from tkinter import simpledialog, ttk, messagebox

    class FilterDialog(simpledialog.Dialog):
        def body(self, master):
            self.title("Add Filter Rule")

            ttk.Label(master, text="Select Field:").grid(row=0, column=0, sticky="w")
            self.field_var = tk.StringVar()
            self.field_combo = ttk.Combobox(master, textvariable=self.field_var, values=header_fields, state="readonly")
            if header_fields:
                # Only set default if nothing is already selected
                if not self.field_var.get().strip():
                    self.field_var.set( header_fields[0] )
                    self.field_combo.current( 0 )

            self.field_combo.grid(row=0, column=1, padx=10, pady=5)

            # DO NOT pre-set the combobox value manually
            # Let the user make the selection without override
            pass

            ttk.Label(master, text="Match Type:").grid(row=1, column=0, sticky="w")
            self.match_var = tk.StringVar(value="equals")
            self.match_combo = ttk.Combobox(master, textvariable=self.match_var,
                                            values=["equals", "contains", "starts_with"], state="readonly")
            self.match_combo.grid(row=1, column=1, padx=10, pady=5)

            ttk.Label(master, text="Match Value(s):").grid(row=2, column=0, sticky="w")
            self.values_entry = ttk.Entry(master)
            self.values_entry.grid(row=2, column=1, padx=10, pady=5)

            return self.field_combo  # initial focus

        def apply(self):
            # Force update in case tkinter didn't finalize the combobox selection
            self.field_var.set( self.field_combo.get() )

            field = self.field_var.get().strip()

            if field not in header_fields:
                print( f"[ERROR] Field '{field}' not found in header list." )
                self.result = []
                return

            print( f"[DEBUG] Selected field from GUI: '{field}'" )

            match_type = self.match_var.get().strip()
            raw_values = self.values_entry.get().strip()
            values = [v.strip().strip( '"' ).strip( "'" ) for v in raw_values.split( "," ) if v.strip()]

            if not field or not values:
                print("[ERROR] Field or values missing.")
                self.result = []
            else:
                self.result = [{
                    "field": field,
                    "match_type": match_type,
                    "value": values
                }]

    rules = []
    root = tk.Tk()
    root.withdraw()

    while True:
        dialog = FilterDialog(root)
        if not dialog.result:
            break
        rules.extend(dialog.result)

        again = messagebox.askyesno("Add Another?", "Would you like to add another filter?")
        if not again:
            break

    root.destroy()
    return rules



def collect_filter_rules_gui(header_columns):
    return get_filter_rules_gui(header_columns)


def collect_filter_rules(header_columns: list[str]) -> list[dict]:
    """
    Prompts user to create filter rules.

    Args:
        header_columns (list[str]): List of available column names for filtering.

    Returns:
        list[dict]: List of filter rule dictionaries to be passed to apply_filters().
    """

    filter_rules = []

    while True:
        print("\nSelect filter type to add:")
        print("  1. Standard text filter (equals, contains, starts_with)")
        print("  2. Date-based filter (with format parsing)")
        print("  3. Numeric filter (equals, <, <=, >, >=, between)")
        if filter_rules:
            print("  4. Done adding filters")
        else:
            print("  4. Cancel without adding any filters")

        choice = input("> ").strip()


        if choice == "1":
            print("\n[STRING FILTER]")
            print( "\nAvailable columns for string filtering:" )
            for idx, col in enumerate( header_columns ):
                print( f"  {idx}: {col}" )
            field_input = input( "Enter the column number or name to filter: " ).strip()
            field_input = field_input.strip().strip( '"' ).strip( "'" )

            if field_input.isdigit():
                index = int( field_input )
                if 0 <= index < len( header_columns ):
                    field = header_columns[index]
                else:
                    print( f"[ERROR] Column index '{index}' is out of range." )
                    continue
            elif field_input in header_columns:
                field = field_input
            else:
                print( f"[ERROR] Column '{field_input}' not found." )
                continue


            print("Select match type:")
            print("  1. Equals")
            print("  2. Contains")
            print("  3. Starts with")
            match_type_map = {"1": "equals", "2": "contains", "3": "starts_with"}
            mt_choice = input("> ").strip()
            match_type = match_type_map.get(mt_choice)
            if not match_type:
                print("[ERROR] Invalid match type selection.")
                continue

            values_input = input("Enter one or more values to match (comma-separated): ").strip()
            values = [v.strip() for v in values_input.split(",") if v.strip()]
            if not values:
                print("[ERROR] No valid values entered.")
                continue

            rule = {
                "field": field,
                "match_type": match_type,
                "value": values
            }
            filter_rules.append(rule)
            print("[INFO] Filter added.")

        elif choice == "2":
            print("\n[DATE FILTER]")
            raw_config = prompt_for_date_filter_config(header_columns)
            parsed_config = validate_and_parse_date_config(raw_config)

            date_rule = {
                "field": parsed_config["column_name"],
                "filter_type": parsed_config["filter_type"],
                "filter_subtype": "date",
                "start_date": parsed_config["start_date"],
                "end_date": parsed_config.get("end_date"),
                "format_code": parsed_config["format_code"]
            }

            filter_rules.append(date_rule)
            print("[INFO] Date filter added.")

        elif choice == "3":
            print("\n[NUMERIC FILTER]")
            print( "\nAvailable columns for numeric filtering:" )
            for idx, col in enumerate( header_columns ):
                print( f"  {idx}: {col}" )

            field_input = input( "Enter the column number or name to filter: " ).strip()
            field_input = field_input.strip().strip( '"' ).strip( "'" )

            if field_input.isdigit():
                index = int( field_input )
                if 0 <= index < len( header_columns ):
                    field = header_columns[index]
                else:
                    print( f"[ERROR] Column index '{index}' is out of range." )
                    continue
            elif field_input in header_columns:
                field = field_input
            else:
                print( f"[ERROR] '{field_input}' not found in column names." )

            print("Select numeric comparison:")
            print("  1. Equals")
            print("  2. Greater than")
            print("  3. Greater than or equal to")
            print("  4. Less than")
            print("  5. Less than or equal to")
            print("  6. Between")
            operator_map = {
                "1": "equals",
                "2": "greater_than",
                "3": "greater_than_or_equal",
                "4": "less_than",
                "5": "less_than_or_equal",
                "6": "between"
            }
            op_choice = input("> ").strip()
            operator = operator_map.get(op_choice)
            if not operator:
                print("[ERROR] Invalid numeric operator.")
                continue

            if operator == "between":
                val1 = input("Enter the lower bound: ").strip()
                val2 = input("Enter the upper bound: ").strip()
                try:
                    num_values = [float(val1), float(val2)]
                except ValueError:
                    print("[ERROR] Both values must be numeric.")
                    continue
            else:
                val = input("Enter the numeric value: ").strip()
                try:
                    num_values = [float(val)]
                except ValueError:
                    print("[ERROR] Invalid numeric value.")
                    continue

            rule = {
                "field": field,
                "match_type": "numeric",
                "operator": operator,
                "value": num_values
            }
            filter_rules.append(rule)
            print("[INFO] Numeric filter added.")

        elif choice == "4":
            break

        else:
            print("[ERROR] Invalid selection.")

    return filter_rules
