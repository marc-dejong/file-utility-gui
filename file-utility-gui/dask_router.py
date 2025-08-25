def handle_dask_mode(shared_data):
    # -- Strip non-serializable Tkinter objects before Dask touches shared_data --
    for key in list(shared_data.keys()):
        if "tkinter" in str(type(shared_data[key])):
            del shared_data[key]

    import os
    import importlib.util
    from tkinter import messagebox
    import dask.dataframe as dd
    from gui_field_selector import gui_select_fields  # ✅ fixed import

    logger = shared_data.get("logger")

    try:
        input_file = shared_data["input_file"]
        delimiter = shared_data["delimiter"]
        has_header = shared_data.get("has_header", True)
        header = 0 if has_header else None
        encoding = shared_data.get("encoding", "utf-8")
        temp_output_file = None

        df = dd.read_csv(input_file, delimiter=delimiter, header=header, encoding=encoding, dtype=str)

        # Apply header if missing
        if not has_header:
            df.columns = [f"COL_{i}" for i in range(len(df.columns))]
            shared_data["header"] = df.columns.tolist()

        for function_name in shared_data["function_queue"]:

            if function_name == "sort_records":
                sort_path = os.path.join(os.path.dirname(__file__), "sorter_7x.py")
                spec = importlib.util.spec_from_file_location("sorter", sort_path)
                sorter = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(sorter)
                print(f"[DEBUG] temp_output_file from sort_records: {temp_output_file}")

                header = shared_data.get("header", [])
                if not header:
                    raise ValueError("Missing header row for sorting.")

                sort_fields = gui_select_fields("Select Fields to Sort By", header)
                if not sort_fields:
                    print("[WARN] No sort fields selected. Skipping sort.")
                    continue

                input_path = shared_data.get("input_file")
                output_path = shared_data.get("output_file")

                if not output_path:
                    logger.warning("[dask_router] No output path from GUI. Using fallback location.")
                    output_path = input_path.replace(".csv", "._RESULTS.csv")

                shared_data["results_written"] = True

                delimiter = shared_data.get("delimiter", ",")
                quote_preserve = not shared_data["flags"].get("strip_quotes", False)
                fallback = shared_data["flags"].get("flexible_decoding", False)

                temp_output_file = sorter.sort_records(
                     df_or_path=input_path,
                     output_file=output_path,
                     sort_fields=sort_fields,
                     delimiter=delimiter,
                     quote_preserve=quote_preserve,
                     fallback=True,
                     logger=logger
                )

                shared_data["output_file"] = temp_output_file  # Set this so GUI can finalize


            elif function_name == "dedupe_records":
                dedupe_path = os.path.join(os.path.dirname(__file__), "deduper_8x.py")
                spec = importlib.util.spec_from_file_location("deduper", dedupe_path)
                deduper = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(deduper)

                header = shared_data.get("header", [])
                if not header:
                    raise ValueError("Missing header row for deduplication.")

                dedupe_fields = shared_data.get("dedupe_fields", [])
                if not dedupe_fields:
                    print("[WARN] No dedupe fields passed from GUI. Skipping deduplication.")
                    continue

                # ✅ GUI prompt for dedupe mode
                from dedupe_ui_8x import prompt_dedupe_mode
                dedupe_mode = prompt_dedupe_mode()
                print(f"[DEBUG] Returned dedupe_mode: {repr(dedupe_mode)}")  # ✅ Add this line

                if not dedupe_mode:
                    print("[INFO] Dedupe mode selection cancelled or empty. Skipping deduplication.")
                    continue

                shared_data["dedupe_mode"] = dedupe_mode
                print(f"[DEBUG] Dedupe mode selected: {dedupe_mode}")  # ✅ ADD THIS

                # Call deduper
                df_result = deduper.dedupe_records(
                    df,
                    dedupe_fields,
                    mode=dedupe_mode,
                    normalize_flag=shared_data.get("normalize", False),
                    logger=logger,
                    shared_data=shared_data
                )

                # ✅ DEBUG row count before writing
                try:
                    row_count = len(df_result)
                except Exception:
                    row_count = "Unknown"
                print(f"[DEBUG] Row count before saving: {row_count}")  # ✅ ADD THIS

                # Set output path
                import tempfile, os
                temp_dir = shared_data.get("temp_dir", tempfile.gettempdir())
                output_path = os.path.join(temp_dir, "dedupe_output.csv")
                temp_output_file = output_path
                shared_data["output_file"] = temp_output_file

                # Save to disk here
                try:
                    df_result.to_csv(output_path, index=False, encoding="utf-8")
                    print(f"[8x_Deduper] Output written to: {output_path}")  # ✅ Confirm write success
                except Exception as e:
                    print(f"[ERROR] Failed to write dedupe output: {e}")

                # Mark output as handled
                shared_data["results_written"] = True

                # Rewrap as Dask DataFrame for pipeline continuity
                import dask.dataframe as dd
                df = dd.from_pandas(df_result, npartitions=1)


            elif function_name == "split_file_by_condition":
                split_path = os.path.join(os.path.dirname(__file__), "splitter_9x_gui.py")
                spec = importlib.util.spec_from_file_location("splitter", split_path)
                splitter = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(splitter)
                splitter.split_file_by_condition(shared_data)
                shared_data["results_written"] = True
                return  # Output already written

            elif function_name == "split_by_composite_condition":
                split_path = os.path.join(os.path.dirname(__file__), "splitter_9x_composite_gui.py")
                spec = importlib.util.spec_from_file_location("splitter", split_path)
                splitter = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(splitter)
                splitter.split_by_composite_condition(shared_data)
                shared_data["results_written"] = True
                return  # Output already written

            elif function_name == "concatenate_files":
                merge_path = os.path.join(os.path.dirname(__file__), "merger_gui_10x.py")
                spec = importlib.util.spec_from_file_location("merger", merge_path)
                merger = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(merger)

                merger.run_merge_10x_process(shared_data, mode="basic")

                shared_data["results_written"] = True
                return  # Output already handled by merger

            elif function_name == "merge_by_key":
                merge_path = os.path.join(os.path.dirname(__file__), "merger_gui_10x.py")
                spec = importlib.util.spec_from_file_location("merger", merge_path)
                merger = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(merger)

                merger.run_merge_10x_process(shared_data, mode="key")

                shared_data["results_written"] = True
                return  # Output handled inside merge_by_key

        # Write final output if not already handled
        if not shared_data.get("results_written"):
            output_path = shared_data["output_file"]
            df.compute().to_csv(
                output_path,
                index=False,
                header=True,
                encoding=encoding,
                lineterminator='\n'  # ✅ prevent blank lines
            )

            print(f"[DASK COMPLETE] Results written to: {output_path}")
        else:
            print("[DASK COMPLETE] Output already written by prior module.")

        if temp_output_file is None:
            print("[WARN] Sort was selected but no output file was generated (likely due to cancel or skip).")

        print(f"[DEBUG] Final return from handle_dask_mode: {temp_output_file}")
        print(f"[DEBUG] temp_output_file after handle_dask_mode: {temp_output_file}")

        return temp_output_file

    except Exception as e:
        messagebox.showerror("Dask Mode Error", f"Dask processing failed:\n{e}")
        shared_data["errors"].append(f"Dask failure: {e}")