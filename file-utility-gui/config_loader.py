import json
import os
from tkinter import filedialog, messagebox


def save_config(shared_data):
    try:
        config_data = {
            "function_queue": shared_data.get("function_queue", []),
            "has_header": shared_data.get("has_header", True),
            "flags": shared_data.get("flags", {}),
            "normalize": shared_data.get("normalize", False),
            "selected_fields": shared_data.get("selected_fields", {}),
            "delimiter": shared_data.get("delimiter", ",")
        }

        file_path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")],
            title="Save Configuration File"
        )

        if not file_path:
            return  # User cancelled

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            json.dump(config_data, f, indent=4)

        messagebox.showinfo("Success", f"Configuration saved to: {file_path}")

    except Exception as e:
        messagebox.showerror("Error Saving Config", str(e))


def load_config():
    try:
        file_path = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json")],
            title="Load Configuration File"
        )

        if not file_path:
            return None  # User cancelled

        with open(file_path, "r", encoding="utf-8", newline="") as f:
            config_data = json.load(f)

        messagebox.showinfo("Success", f"Configuration loaded from: {file_path}")
        return config_data

    except Exception as e:
        messagebox.showerror("Error Loading Config", str(e))
        return None
