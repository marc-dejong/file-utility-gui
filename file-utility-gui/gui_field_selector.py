import tkinter as tk
from tkinter import messagebox

def gui_select_fields(title, header, prompt=None):

    win = tk.Toplevel()
    win.title(title)
    win.geometry("450x500")

    tk.Label(win, text=prompt or "Select field(s) from list OR enter manually below:").pack(pady=(10, 5))


    # Build listbox with "index: name"
    selected_fields_order = []
    indexed_fields = [f"{i}: {name}" for i, name in enumerate(header)]

    listbox = tk.Listbox(win, selectmode=tk.MULTIPLE, exportselection=False, height=15)

    def on_select_field(event):
        widget = event.widget
        selections = widget.curselection()
        for idx in selections:
            try:
                field_line = widget.get(idx)
                field_name = field_line.split(": ", 1)[1]
                if field_name not in selected_fields_order:
                    selected_fields_order.append(field_name)
            except IndexError:
                continue

    listbox.bind("<<ListboxSelect>>", on_select_field)
    for item in indexed_fields:
        listbox.insert(tk.END, item)
    listbox.pack(fill=tk.BOTH, expand=False, padx=10)

    # Manual entry box
    tk.Label(win, text="...or enter column numbers or names (comma-separated):").pack(pady=(10, 5))
    manual_entry = tk.Entry(win, width=50)
    manual_entry.pack(pady=(0, 10))

    selected = []

    def on_submit():
        selected_items = listbox.curselection()
        raw_input = manual_entry.get().strip()

        # From listbox
        for idx in selected_items:
            raw = indexed_fields[idx]
            col_name = raw.split(": ", 1)[1]
            selected.append(col_name)

        # From manual entry
        if raw_input:
            items = [item.strip() for item in raw_input.split(",") if item.strip()]
            for item in items:
                if item.isdigit():
                    idx = int(item)
                    if 0 <= idx < len(header):
                        selected.append(header[idx])
                    else:
                        messagebox.showerror("Invalid Index", f"Index {idx} is out of range.")
                        selected.clear()
                        return
                elif item in header:
                    selected.append(item)
                else:
                    messagebox.showerror("Invalid Field", f"'{item}' is not a valid field name.")
                    selected.clear()
                    return

        if not selected:
            messagebox.showwarning("No Selection", "Please select or enter at least one field.")
            return

        win.destroy()

    tk.Button(win, text="OK", command=on_submit).pack(pady=10)
    win.wait_window()

    return selected
