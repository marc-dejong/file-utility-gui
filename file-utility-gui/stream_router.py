def handle_stream_mode(shared_data, record_stream):
    import importlib.util
    import os
    import csv
    from tkinter import messagebox, filedialog
    import logging
    # normalizer.py
    from normalizer_core import normalize_streaming_record

    # Ensure errors list exists and use a named logger for clarity
    shared_data.setdefault("errors", [])
    logger = logging.getLogger("stream_router")
    shared_data.setdefault("_replacer_module", None)

    def _ensure_header_once(outfile, shared_data, logger):
        if not shared_data.get("header_written") and shared_data.get("output_header") and shared_data.get(
                "write_header", True):
            delim = shared_data.get("output_delimiter", ";")
            header_line = delim.join(map(str, shared_data["output_header"])) + "\n"
            outfile.write(header_line)
            shared_data["header_written"] = True
            if logger:
                logger.info(f"[HEADER] Wrote composite header ({len(shared_data['output_header'])} cols)")

    def _write_record(outfile, record, delimiter, quote_map, logger, shared_data, csv_writer=None):
        # Always write header first (once)
        _ensure_header_once(outfile, shared_data, logger)

        # Optional sanity check
        oh = shared_data.get("output_header")
        if oh and len(record) != len(oh) and logger:
            logger.warning(f"[STUB] Row length {len(record)} != header length {len(oh)}")

        # Try to preserve the original field-level quoting
        try:
            quoted_record = []
            for i, val in enumerate(record):
                s = "" if val is None else str(val)
                if i < len(quote_map) and quote_map[i]:
                    s = s.replace('"', '""')
                    quoted_record.append(f'"{s}"')
                else:
                    quoted_record.append(s)
            outfile.write(delimiter.join(quoted_record) + "\n")
        except Exception as e:
            if logger:
                logger.warning(f"[QUOTE FALLBACK] Failed to write record: {e}")
            if csv_writer is not None:
                csv_writer.writerow(record)
            else:
                # last-resort fallback
                outfile.write(delimiter.join(map(lambda x: "" if x is None else str(x), record)) + "\n")

    def normalize_record(record, config):
        return normalize_streaming_record(record, config)

    # Prepare output file
    output_path = shared_data["output_file"]
    delimiter = shared_data["delimiter"]
    # Ensure header emission uses the same delimiter as row writes
    shared_data.setdefault("output_delimiter", delimiter)

    header = shared_data.get("header", [])
    has_header = shared_data.get("has_header", True)
    encoding = shared_data.get("encoding", "utf-8")

    print("[DEBUG] Entered stream_router")
    logger.info("[DEBUG] Entered stream_router")
    quote_map = shared_data.get("quote_map", [])

    try:
        with open(output_path, mode="w", newline="", encoding=encoding) as outfile:
            # If a composite header exists, force the deferred header writer path
            if shared_data.get("output_header") and shared_data.get("write_header") is False:
                logger.info("[HEADER] Forcing write_header=True because composite header is present")
                shared_data["write_header"] = True

            # Try to auto-compose a composite header if the GUI didn't publish one
            if shared_data.get("write_header") and not shared_data.get("output_header"):
                stub_names = (
                    shared_data.get("stub_field_names")
                    or shared_data.get("chosen_stub_names")  # alternate key, if used
                )
                if stub_names:
                    try:
                        shared_data["output_header"] = list(header) + list(stub_names)
                        _preview = ", ".join(map(str, shared_data["output_header"][:5]))
                        logger.info(
                            f"[HEADER] Auto-composed composite header ({len(shared_data['output_header'])} cols) [{_preview} ...]")

                    except Exception as _hdr_err:
                        logger.warning(f"[HEADER] Failed to auto-compose composite header: {_hdr_err}")

            writer = csv.writer(
                outfile,
                delimiter=delimiter,
                quoting=csv.QUOTE_NONE,
                escapechar='\\'  # <- add this one char escape to avoid fallback errors
            )

            # --- One-time header policy ---
            # --- Defer header emission to _ensure_header_once/_write_record ---
            if shared_data.get("output_header"):
                shared_data["write_header"] = True
                shared_data.setdefault("header_written", False)
                try:
                    logger.info(f"[HEADER] Using composite header ({len(shared_data['output_header'])} cols)")
                except Exception:
                    logger.info("[HEADER] Using composite header (unknown cols)")

            # No composite header yet, but caller wants a header → use input header as output_header.
            elif shared_data.get("write_header"):
                shared_data["output_header"] = header
                shared_data.setdefault("header_written", False)

            # Legacy path: header writing explicitly disabled → write raw input header immediately,
            # BUT ONLY if no composite header is present.
            else:
                if has_header and header and not shared_data.get("output_header"):
                    writer.writerow(header)
                    try:
                        cols = len(header)
                    except Exception:
                        cols = 'unknown'
                    logger.info(f"[HEADER] Wrote input header ({cols} columns)")

            # ------------------------------------------------------------
            # PRELOAD FILTER ENGINE & RULES (only once, before streaming)
            # ------------------------------------------------------------
            fq = set(shared_data.get("function_queue", []))

            filters_mod = None
            if "filter_select" in fq or "filter_omit" in fq:
                # Load filter engine once
                filter_path = os.path.join(os.path.dirname(__file__), "filters_3x_gui.py")
                spec = importlib.util.spec_from_file_location("filters", filter_path)
                filters_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(filters_mod)

                # Prefer rules collected earlier in on_continue()
                # Fallback to legacy keys if present
                rules_from_gui = shared_data.get("filter_rules")
                rules_select = rules_from_gui or shared_data.get("filter_select_rules")
                rules_omit   = rules_from_gui or shared_data.get("filter_omit_rules")

                # If still missing, collect ONCE here (not inside the loop)
                if "filter_select" in fq and not rules_select:
                    from filters_ui_3x_gui import collect_filter_rules_gui
                    hdr = shared_data.get("header", [])
                    rules_select = collect_filter_rules_gui(hdr)
                    shared_data["filter_select_rules"] = rules_select or []
                if "filter_omit" in fq and not rules_omit:
                    from filters_ui_3x_gui import collect_filter_rules_gui
                    hdr = shared_data.get("header", [])
                    rules_omit = collect_filter_rules_gui(hdr)
                    shared_data["filter_omit_rules"] = rules_omit or []

                # Logging for visibility
                try:
                    sel_cnt = 0 if not rules_select else len(rules_select)
                    omi_cnt = 0 if not rules_omit else len(rules_omit)
                    logger.info(f"[FILTER] Prepared rules — select:{sel_cnt} omit:{omi_cnt}")
                except Exception:
                    pass

            # Convenience locals
            _flags = dict(shared_data.get("flags", {}))  # copy so we can add defaults safely
            _flags.setdefault("strip_quotes", True)
            _flags.setdefault("case_sensitive", False)
            _header = shared_data.get("header", [])


            # Counters for visibility
            kept_count = 0
            dropped_count = 0
            replaced_rows = 0


            for row in record_stream:
                record = row
                wrote_via_stub = False

                # Apply normalization if enabled
                if shared_data.get("normalize"):
                    try:
                        record = normalize_record(record, shared_data)
                    except Exception as norm_err:
                        print(f"[WARNING] Normalization error: {norm_err}")
                        shared_data["errors"].append(f"Normalization failure: {norm_err}")
                        continue

                # Apply all selected functions
                for function_name in shared_data.get("function_queue", []):
                    if record is None:
                        break

                    if function_name == "filter_select":
                        if filters_mod and (shared_data.get("filter_rules") or shared_data.get("filter_select_rules")):
                            rules_sel = shared_data.get("filter_rules") or shared_data.get("filter_select_rules")

                            # One-time debug to confirm what we are comparing
                            if not shared_data.get("_filter_debug_logged"):
                                try:
                                    fld = rules_sel[0]["field"]
                                    idx = _header.index(fld) if isinstance(fld, str) and fld in _header else int(fld)
                                    logger.debug(f"[FILTER DBG] Field '{fld}' sample raw: {record[idx]!r}")
                                    logger.debug(f"[FILTER DBG] Flags: strip_quotes={_flags.get('strip_quotes')} "
                                                 f"case_sensitive={_flags.get('case_sensitive')}")
                                    logger.debug(f"[FILTER DBG] Rule: {rules_sel[0]}")
                                except Exception as _e:
                                    logger.debug(f"[FILTER DBG] could not sample field: {_e}")
                                shared_data["_filter_debug_logged"] = True

                            # logger.debug("[FILTER] Running filter_select")
                            matched = filters_mod.apply_filters(record, rules_sel, _flags, _header)
                            if not matched:
                                record = None  # skip non-matching record
                                break

                    elif function_name == "filter_omit":
                        if filters_mod and (shared_data.get("filter_rules") or shared_data.get("filter_omit_rules")):
                            rules_omi = shared_data.get("filter_rules") or shared_data.get("filter_omit_rules")
                            # logger.debug("[FILTER] Running filter_omit")
                            matched = filters_mod.apply_filters(record, rules_omi, _flags, _header)
                            if matched:
                                record = None  # skip matching record (omit)
                                break

                    elif function_name == "replace_rec_contents":
                        # --- STEP 1: GUI to collect config (once) ---
                        if "replace_config" not in shared_data:
                            from replacer_ui_4x import get_replacement_config_gui
                            header = shared_data.get("header", [])
                            parent = shared_data.get("root")  # parent the dialog to the main window
                            try:
                                config = get_replacement_config_gui(header, parent=parent)
                            except TypeError:
                                # back-compat if function signature not updated
                                config = get_replacement_config_gui(header)
                            print("[DEBUG] get_replacement_config_gui has returned")
                            logger.debug("[DEBUG] GUI config returned from get_replacement_config_gui")
                            if not config:
                                logger.warning("No replacement config collected. Skipping replace step.")
                                continue
                            shared_data["replace_config"] = config

                        # --- STEP 2: Load replacer logic ONCE and cache it ---
                        if shared_data.get("_replacer_module") is None:
                            replace_path = os.path.join(os.path.dirname(__file__), "4x_Replacer_gui.py")
                            spec = importlib.util.spec_from_file_location("replacer", replace_path)
                            mod = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(mod)
                            shared_data["_replacer_module"] = mod
                            logger.debug("[REPLACER] Loaded 4x_Replacer_gui.py module once")

                        replacer = shared_data["_replacer_module"]

                        # --- STEP 3: Apply the actual replacement for THIS record ---
                        record, was_changed = replacer.replace_rec_contents(
                            record,
                            shared_data.get("header", []),
                            shared_data["replace_config"]
                        )
                        if was_changed:
                            replaced_rows += 1


                    elif function_name == "add_rec_stub_(fixed)":
                        stub_path = os.path.join(os.path.dirname(__file__), "stub_5x_gui.py")
                        spec = importlib.util.spec_from_file_location("stubber", stub_path)
                        stubber = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(stubber)
                        record = stubber.add_fixed_stub(record, shared_data)

                    elif function_name == "add_rec_stub_(var_from_rec_contents)":
                        logger.debug(f"[ROUTER] Stub dispatch triggered for function: {function_name}")
                        # --- Only load config once ---
                        if "stub_config" not in shared_data or not shared_data.get("stub_config"):
                            # Dynamically import stub UI and logic modules
                            stub_ui_path = os.path.join(os.path.dirname(__file__), "stub_5x_ui_gui.py")
                            spec_stub_ui = importlib.util.spec_from_file_location("stub_ui", stub_ui_path)
                            stub_ui = importlib.util.module_from_spec(spec_stub_ui)
                            spec_stub_ui.loader.exec_module(stub_ui)

                            stub_logic_path = os.path.join(os.path.dirname(__file__), "stub_5x_gui.py")
                            spec_stub_logic = importlib.util.spec_from_file_location("stub_logic", stub_logic_path)
                            stub_logic = importlib.util.module_from_spec(spec_stub_logic)
                            spec_stub_logic.loader.exec_module(stub_logic)

                            print("[DEBUG] Using get_stub_file_mapping_config() for stub config")
                            logger.info("[DEBUG] Using get_stub_file_mapping_config() for stub config")

                            # Prompt for stub file
                            stub_file = filedialog.askopenfilename(
                                title="Select stub file",
                                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
                            )

                            logger.debug(f"[ROUTER] stub_file: {stub_file}")

                            if not stub_file:
                                messagebox.showwarning("Stub Cancelled", "No stub file selected.")
                                return

                            # Convert field-name-based mapping from GUI into index-based stub_config
                            config = stub_ui.get_stub_file_mapping_config_from_gui_mapping(
                                shared_data.get("stub_field_mapping", {}),
                                logger=shared_data.get("logger")
                            )

                            logger.debug(f"[ROUTER] stub_config keys: {list(config.keys()) if config else 'None'}")

                            if config is None:
                                messagebox.showwarning("Stub Cancelled", "No stub configuration provided.")
                                return

                            shared_data["stub_config"] = config
                            shared_data["stub_file"] = config.get("stub_file_path")
                        else:
                            # If stub config was already stored, we still need logic module
                            stub_logic_path = os.path.join(os.path.dirname(__file__), "stub_5x_gui.py")
                            spec_stub_logic = importlib.util.spec_from_file_location("stub_logic", stub_logic_path)
                            stub_logic = importlib.util.module_from_spec(spec_stub_logic)
                            spec_stub_logic.loader.exec_module(stub_logic)

                        results = stub_logic.dispatch_stub_logic(
                            record,
                            shared_data["stub_config"],
                            record_index=None,
                            normalize_config=shared_data.get("normalize_config"),
                            log_fn=shared_data.get("logger")
                        )

                        try:
                            logger.debug(f"[STUB] Fan-out results count: {0 if not results else len(results)}")
                        except Exception:
                            pass

                        if results and all(isinstance(r, (list, tuple)) and len(r) == 2 for r in results):
                            for output_record, _ in results:
                                # Route all fan-out writes through unified writer (emits header once)
                                _write_record(outfile, output_record, delimiter, quote_map, logger, shared_data, csv_writer=writer)
                                wrote_via_stub = True
                        else:
                            logger.warning("[STUB] dispatch_stub_logic() returned empty or malformed results.")

                    elif function_name == "delete_rec_by_condition":
                        deleter_path = os.path.join(os.path.dirname(__file__), "6x_Deleter.py")
                        spec = importlib.util.spec_from_file_location("deleter", deleter_path)
                        deleter = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(deleter)
                        if deleter.should_delete(record, shared_data):
                            record = None
                            break

                # After processing all functions, write record if still valid.
                # If stub fan-out already emitted rows, we don't count the base record as dropped.
                if record is not None and not wrote_via_stub:
                    _write_record(outfile, record, delimiter, quote_map, logger, shared_data, csv_writer=writer)
                    kept_count += 1
                elif record is None:
                    dropped_count += 1
                # else: wrote_via_stub == True → treated as handled; neither kept_count nor dropped_count.

            # --- Ensure a header is still written even if all rows were filtered out ---
            if (shared_data.get("write_header")
                and shared_data.get("output_header")
                and not shared_data.get("header_written")):
                try:
                    delim = shared_data.get("output_delimiter", delimiter)
                    outfile.write(delim.join(map(str, shared_data["output_header"])) + "\n")
                    shared_data["header_written"] = True
                    logger.info("[HEADER] (filters) Wrote header with zero matching rows")
                except Exception as _h0:
                    logger.warning(f"[HEADER] Fallback header write failed: {_h0}")

            # helpful summary
            logger.info(f"[FILTER] Kept rows: {kept_count}  |  Dropped rows: {dropped_count}")
            logger.info(f"[REPLACE] Rows updated: {replaced_rows}")

        # existing completion logs
        print(f"[STREAM COMPLETE] Results written to: {output_path}")
        logger.info(f"[STREAM] Final output saved to: {output_path}")  # <-- also change tag here

        print(f"[DEBUG] ROOT TYPE: {type(shared_data.get('root'))}")
        print(f"[DEBUG] ROOT INSTANCE: {shared_data.get('root')}")

    except Exception as e:
        messagebox.showerror("Stream Mode Error", f"Streaming operation failed:\n{e}")
        shared_data["errors"].append(f"Stream failure: {e}")
