
def function_routing(shared_data):
    import os
    import csv
    import importlib.util
    from tkinter import messagebox
    from utils_11x_gui import safe_open
    from itertools import chain
    from file_util_gui_0x import apply_stub_logic
    from Two_File_Compare_gui import launch_compare_gui
    import logging
    logger = logging.getLogger()

    try:
        import dask
        import dask.dataframe as dd
    except ImportError:
        dd = None

    sample_rows = None

    print("[DEBUG] Entered function_routing")
    logger.info("[DEBUG] Entered function_routing")

    # Special case: handle file_compare immediately
    if "file_compare" in shared_data.get("function_queue", []):
        try:
            launch_compare_gui()
            return  # Stop further processing
        except Exception as e:
            messagebox.showerror("File Compare Error", f"Compare failed:\n{e}")
            shared_data["errors"].append("File compare failed.")
            return

    # === Diagnostic Dump ===
    try:
        logger.info("[DIAG] Function Queue: %s", shared_data.get("function_queue"))
        logger.info("[DIAG] Input File: %s", shared_data.get("input_file"))
        logger.info("[DIAG] Output File: %s", shared_data.get("output_file"))
        logger.info("[DIAG] Delimiter: '%s'", shared_data.get("delimiter"))
        logger.info("[DIAG] Header Fields: %s", shared_data.get("header_fields"))
        logger.info("[DIAG] Normalize Flag: %s", shared_data.get("normalize"))
    except Exception as diag_err:
        logger.warning("[DIAG] Failed during diagnostic dump: %s", diag_err)

    # Detect delimiter
    file_loader_spec = importlib.util.spec_from_file_location("file_loader", "./2x_File_Loader_gui.py")
    file_loader = importlib.util.module_from_spec(file_loader_spec)
    file_loader_spec.loader.exec_module(file_loader)

    shared_data["delimiter"] = file_loader.detect_delimiter(shared_data["input_file"], shared_data)
    # After detect_delimiter(...)
    try:
        with open(shared_data["input_file"], "r", encoding="utf-8", newline="") as f:
            first = f.readline()
        d = shared_data["delimiter"] or ","
        if ";" in first and (d != ";" or first.count(";") >= first.count(",")):
            d = ";"
        elif "," in first and d != "," and first.count(",") > 0 and first.count(";") == 0:
            d = ","
        elif "\t" in first and d != "\t":
            d = "\t"
        if d != shared_data["delimiter"]:
            logger.info("[router] Adjusting detected delimiter: '%s' -> '%s'", shared_data["delimiter"], d)
            shared_data["delimiter"] = d
    except Exception as e:
        logger.warning("[router] Early delimiter resolve failed: %s", e)

    print(f"[INFO] Detected delimiter: '{shared_data['delimiter']}'")

    # Prime header
    # === Step 1: Prime header before normalization ===
    try:
        record_stream = file_loader.read_file(shared_data["input_file"], shared_data, shared_data["delimiter"])
        first_row = next(record_stream)

        if shared_data["has_header"]:
            shared_data["header"] = first_row
            shared_data["header_fields"] = first_row
        else:
            # Create synthetic column names and put the consumed first_row back without materializing
            shared_data["header"] = [f"COL_{i}" for i in range(len(first_row))]
            shared_data["header_fields"] = shared_data["header"]
            record_stream = chain([first_row], record_stream)


    except StopIteration:
        messagebox.showerror("File Error", "The input file appears to be empty.")
        shared_data["errors"].append("Empty input file.")
        return
    except Exception as e:
        messagebox.showerror("Read Error", f"Failed to read file header: {e}")
        shared_data["errors"].append("Header priming failed.")
        return

    # === Step 2: Normalize file if selected ===
    if shared_data.get("normalize"):
        from normalizer_core import normalize_streaming_record, generate_linkage_config
        import tempfile

        logger = shared_data.get("logger") or logging.getLogger()
        logger.info("[INFO] Normalization enabled. Generating normalization config...")

        # Sample ~25 rows for config
        sample_rows = []
        infile, _, _ = safe_open(shared_data["input_file"], mode="r")
        with infile:
            reader = csv.reader(infile, delimiter=shared_data["delimiter"])
            if shared_data.get("has_header"):
                _ = next(reader)  # skip header
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i < 10:
                    logger.debug(f"[NORMALIZE] Sample row {i}: {row}")
                if i >= 25:
                    break

        shared_data["sample_rows"] = sample_rows

        # Now: Use field names and build config
        field_names = shared_data.get("header", [])
        if not sample_rows or not field_names:
            raise ValueError("Missing sample_rows or field_names for normalization.")

        norm_config = generate_linkage_config(sample_rows, field_names)
        norm_config["sample_records"] = sample_rows  # required for streaming normalize

        # Rewrite normalized version
        normalized_path = tempfile.mktemp(suffix="_normalized.csv")
        infile, _, _ = safe_open(shared_data["input_file"], mode="r")
        outfile, _, _ = safe_open(normalized_path, mode="w", newline="")

        with infile, outfile:
            reader = csv.reader(infile, delimiter=shared_data["delimiter"])
            writer = csv.writer(outfile, delimiter=shared_data["delimiter"])

            if shared_data.get("has_header"):
                header = next(reader)
                writer.writerow(header)

            for row in reader:
                norm_row = normalize_streaming_record(row, norm_config)
                writer.writerow(norm_row)

        logger.info(f"[INFO] Normalized file written to: {normalized_path}")
        shared_data["input_file"] = normalized_path
        shared_data["normalize_config"] = norm_config
        # Recreate record_stream to stream from the normalized file (skip header once)
        record_stream = file_loader.read_file(shared_data["input_file"], shared_data, shared_data["delimiter"])
        try:
            if shared_data.get("has_header"):
                _ = next(record_stream)  # we already primed header above
        except StopIteration:
            messagebox.showerror("File Error", "The normalized input file appears to be empty.")
            shared_data["errors"].append("Empty normalized input file.")
            return

        print(f"[DEBUG] Selected function queue: {shared_data.get('function_queue')}")
        logger.debug(f"[DEBUG] Selected function queue: {shared_data.get('function_queue')}")

    else:
        # Normalization not selected — explicitly clear these fields
        shared_data["normalize_config"] = None
        shared_data["sample_rows"] = None
        shared_data["field_names"] = None

    # Determine execution mode
    stream_functions = {
        "filter_omit", "filter_select", "replace_rec_contents",
        "add_rec_stub_(fixed)", "add_rec_stub_(var_from_rec_contents)",
        "delete_rec_by_condition"
    }

    dask_functions = {
        "sort_records", "dedupe_records",
        "split_file_by_condition", "split_by_composite_condition",
        "concatenate_files", "merge_by_key"
    }

    selected = set(shared_data["function_queue"])
    uses_stream = any(f in stream_functions for f in selected)
    uses_dask = any(f in dask_functions for f in selected)

    if uses_stream and uses_dask:
        messagebox.showerror("Invalid Function Mix", "You selected a mix of incompatible functions (stream + dask). Please separate them.")
        return

    if uses_stream:
        shared_data["mode"] = "stream"
    elif uses_dask:
        shared_data["mode"] = "dask"
    else:
        messagebox.showerror("Unknown Mode", "Could not determine execution mode based on selected functions.")
        return

    logger.info("[DIAG] Mode (resolved): %s", shared_data.get("mode"))

    # Call into main execution block
    if shared_data["mode"] == "stream":
        # Special case: handle stub logic directly here
        print(f"[DEBUG] STREAM Functions selected: {shared_data.get('function_queue')}")
        if "add_rec_stub_(var_from_rec_contents)" in shared_data["function_queue"]:
            stub_file = shared_data.get("external_stub_file")
            if not stub_file:
                messagebox.showerror("Missing Stub File", "No stub file provided in configuration.")
                return

            if "stub_field_mapping" not in shared_data:
                messagebox.showerror("Mapping Missing", "No field mapping found in shared data.")
                return

            # Use GUI-based mapping converter instead of manual mapping
            from stub_5x_ui_gui import get_stub_file_mapping_config_from_gui_mapping

            mapping = shared_data.get("stub_field_mapping", {})
            logger = shared_data.get("logger") or logging.getLogger()

            config = get_stub_file_mapping_config_from_gui_mapping(mapping, logger=logger)

            # ✅ PATCH: explicitly attach stub file + output path info
            config["stub_file"] = shared_data.get("external_stub_file")
            config["input_file"] = shared_data.get("input_file")
            config["output_file"] = shared_data.get("output_file")

            shared_data["stub_config"] = config

            normalize_config = shared_data.get("normalize_config")

            # Detect field quote structure
            try:
                with open(shared_data["input_file"], "r", encoding="utf-8", newline="") as f:
                    first_line = next(f).strip()
                    delimiter = shared_data.get("delimiter", ",")
                    quote_flags = [
                        field.startswith('"') and field.endswith('"')
                        for field in first_line.split(delimiter)
                    ]
                    shared_data["field_quote_flags"] = quote_flags
                    logger.debug(f"[DEBUG] Field-level quote structure: {quote_flags}")
            except Exception as e:
                logger.warning(f"[WARNING] Failed to detect quote flags: {e}")
                shared_data["field_quote_flags"] = []

            if "stub_config" in shared_data:
                logger = shared_data.get("logger") or logging.getLogger()
                config = shared_data["stub_config"]
                if logger:
                    logger.debug(f"[VERIFY] stub_config before apply_stub_logic: {config}")
                if not config.get("stub_fields"):
                    raise ValueError("[FATAL] stub_config['stub_fields'] is missing or empty")
            else:
                raise ValueError("[FATAL] shared_data['stub_config'] is missing entirely")

            if "stub_config" in shared_data:
                logger.debug(f"[STUB] stub_config before apply_stub_logic: {shared_data['stub_config']}")
            else:
                logger.error("[STUB] stub_config is missing from shared_data")

            # Run stub logic
            temp_output_file = apply_stub_logic(
                input_path=shared_data["input_file"],
                output_path=shared_data["output_file"],
                config=config,
                normalize_config=normalize_config,
                logger=logger,
                shared_data=shared_data
            )

            if temp_output_file and os.path.exists(temp_output_file):
                logger.info(f"[INFO] Stub output successfully written to: {temp_output_file}")
                shared_data["results_written"] = True
                shared_data["final_output_path"] = temp_output_file
            else:
                logger.error("[ERROR] Stub logic did not write an output file as expected.")
                shared_data["errors"].append("Stub logic failed to produce output.")
            return

        # Fallback: general stream handling
        from stream_router import handle_stream_mode
        import tkinter as tk
        # Ensure robust text comparison defaults for filtering
        flags = shared_data.setdefault("flags", {})
        flags.setdefault("strip_quotes", True)
        flags.setdefault("case_sensitive", False)

        handle_stream_mode(shared_data, record_stream)

        # Clean up GUI
        try:
            if tk._default_root is not None:
                tk._default_root.quit()
                tk._default_root.destroy()
        except Exception as e:
            print(f"[WARNING] GUI close failed: {e}")

    elif shared_data["mode"] == "dask":
        # Special-case dedupe_records: run engine directly; otherwise fall back to dask router
        if "dedupe_records" in shared_data.get("function_queue", []):
            try:
                from deduper_8x import dedupe_records

                # Read cached values from GUI (do NOT open any dialogs here)
                dedupe_mode = shared_data.get("dedupe_mode")
                dedupe_fields = shared_data.get("dedupe_fields") or []

                if dedupe_mode not in ("dedupe", "keep_duplicates_only"):
                    raise ValueError("Missing dedupe_mode in shared_data (GUI should have cached it).")
                if not dedupe_fields:
                    raise ValueError("Missing dedupe_fields in shared_data (GUI should have cached them).")

                # --- Read CSV correctly to avoid column/key mismatches ---
                sep = shared_data.get("delimiter", ",")
                has_header = bool(shared_data.get("has_header", True))
                read_kwargs = dict(
                    sep=sep,
                    dtype=str,
                    assume_missing=True,
                    blocksize="32MB",
                    on_bad_lines="skip",
                )
                if has_header:
                    df = dd.read_csv(shared_data["input_file"], header=0, **read_kwargs)
                else:
                    df = dd.read_csv(shared_data["input_file"], header=None, **read_kwargs)

                # Fallback: if we only got 1 column, re-sniff using the first line
                try:
                    h = df.head(1)
                    if len(h.columns) == 1:
                        with open(shared_data["input_file"], "r", encoding="utf-8", newline="") as f:
                            first_line = f.readline()
                        if ";" in first_line and sep != ";":
                            logger.info("[function_router] Single-column read detected; retry with ';'")
                            sep = ";"
                        elif "," in first_line and sep != ",":
                            logger.info("[function_router] Single-column read detected; retry with ','")
                            sep = ","
                        if sep != shared_data.get("delimiter"):
                            shared_data["delimiter"] = sep
                        read_kwargs["sep"] = sep
                        if has_header:
                            df = dd.read_csv(shared_data["input_file"], header=0, **read_kwargs)
                        else:
                            df = dd.read_csv(shared_data["input_file"], header=None, **read_kwargs)
                except Exception as _fallback_e:
                    logger.warning(f"[function_router] Delimiter fallback check failed: {_fallback_e}")

                # Final delimiter used (for visibility)
                logger.info("[function_router] Using delimiter for Dask read: '%s'", sep)

                # Diagnostics
                try:
                    h = df.head(1)
                    logger.info(f"[function_router] Dask df columns: {len(df.columns)}, head width: {len(h.columns)}")
                except Exception as diag_e:
                    logger.warning(f"[function_router] head() failed during CSV read: {diag_e}")

                # Row count (excluding header)
                try:
                    nrows = int(df.map_partitions(len).sum().compute())
                    logger.info(f"[function_router] Row count (excluding header): {nrows}")
                except Exception as cnt_e:
                    logger.warning(f"[function_router] Row count check failed: {cnt_e}")

                # Preflight: ensure selected fields exist after ingest
                try:
                    cols = list(df.columns)
                    missing = [c for c in dedupe_fields if c not in cols]
                    if missing:
                        logger.error("[function_router] Dedupe fields missing after ingest: %s", missing)
                        logger.error("[function_router] Available columns: %s", cols)
                        messagebox.showerror(
                            "Dedupe Error",
                            "Selected dedupe columns not found in ingested file:\n"
                            f"{missing}\n\nAvailable columns:\n{cols}"
                        )
                        return
                except Exception as pre_e:
                    logger.warning("[function_router] Preflight column check failed: %s", pre_e)

                # Run dedupe engine (no UI inside)
                result = dedupe_records(
                    df,
                    dedupe_fields,
                    mode=dedupe_mode,
                    normalize_flag=bool(shared_data.get("normalize", False)),
                    logger=shared_data.get("logger"),
                    shared_data=shared_data,
                )

                # === Choose output path + force delimiter ===
                sep_out = shared_data.get("delimiter") or shared_data.get("output_delimiter") or ";"

                in_base = os.path.splitext(os.path.basename(shared_data["input_file"]))[0]
                out_name = f"{in_base}._DEDUPED.csv"
                out_path = os.path.join(shared_data.get("output_folder", os.getcwd()), out_name)
                shared_data["output_file"] = out_path

                # === Write result as a single file with the chosen delimiter ===
                # If result is a Dask DataFrame (keep_duplicates_only path), compute to pandas first for consistent sep
                try:
                    if dd is not None and isinstance(result, dd.DataFrame):
                        result = result.compute()
                except Exception:
                    pass

                # Now 'result' is pandas.DataFrame
                result.to_csv(out_path, index=False, sep=sep_out)
                shared_data["results_written"] = True
                logger.info(f"[8x_Deduper] Output written to: {shared_data['output_file']} (sep='{sep_out}')")
                return


            except Exception as e:
                import traceback
                messagebox.showerror("Dedupe Error", f"Deduplication failed:\n{e}")
                shared_data["errors"].append(f"Dedupe error: {str(e)}\n{traceback.format_exc()}")
                return

        # Default Dask path for all other functions
        from dask_router import handle_dask_mode
        temp_output_file = handle_dask_mode(shared_data)
        shared_data["output_file"] = temp_output_file
        return

    elif shared_data["mode"] == "standalone":
        # Already routed above
        pass
    else:
        print("[ERROR] Unrecognized mode after routing logic.")
        shared_data["errors"].append("Unrecognized execution mode.")
