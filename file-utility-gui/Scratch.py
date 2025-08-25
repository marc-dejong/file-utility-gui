# ✅ Prompt user for fixed stub value if fixed mode selected
            if "add_rec_stub_(fixed)" in self.shared_data["function_queue"]:
                try:
                    import importlib.util
                    config_path = os.path.join(os.path.dirname(__file__), "stub_5x_ui_config_gui.py")
                    spec = importlib.util.spec_from_file_location("stub_config", config_path)
                    stub_config = importlib.util.module_from_spec(spec)

                    if self.shared_data.get("logger"):
                        self.shared_data["logger"].debug(
                            f"[FINAL MAPPING] stub_fields = {self.shared_data.get('stub_config', {}).get('stub_fields')}")
                        self.shared_data["logger"].debug(
                            f"[FINAL MAPPING] full stub_config = {self.shared_data.get('stub_config')}")

                    spec.loader.exec_module(stub_config)

                    result = stub_config.get_stub_column_config(self.shared_data.get("header", []))

                    if result and result.get("value") is not None:
                        self.shared_data["fixed_stub_fields"] = [result["value"]]
                        self.shared_data["fixed_stub_field_name"] = result["new_column"]
                    else:
                        messagebox.showwarning("Stub Cancelled", "No fixed stub value provided.")
                        return

                except Exception as e:
                    messagebox.showerror("Fixed Stub Error", f"Failed to configure fixed stub:\n{e}")
                    return