#
import logging
logger = logging.getLogger()

# print("[DEBUG] Entered replacer gui")
# logger.info("[DEBUG] Entered replacer_gui_")


def replace_rec_contents(row, header_fields, replacement_config, log_fn=None):
    """
    Applies replacement rules to a single row based on the configuration.
    If log_fn is provided, logs changes.
    Returns a tuple: (modified_row, was_changed)
    """
    updated_row = row.copy()
    was_changed = False

    for rule in replacement_config.get( "rules", [] ):
        field_name = rule["field"]
        old_value = rule["old_value"]
        new_value = rule["new_value"]
        match_type = rule["match_type"]
        case_sensitive = rule["case_sensitive"]

        try:
            col_idx = header_fields.index( field_name )
        except ValueError:
            continue

        current_value = updated_row[col_idx]

        # Rule-specific quote handling (used only during comparison)
        compare_val = current_value if case_sensitive else current_value.lower()
        match_target = old_value if case_sensitive else old_value.lower()

        # Always trim extra whitespace
        current_value = current_value.strip()

        # Only strip quotes if rule requests it
        compare_val = current_value
        match_target = old_value
        if rule.get("strip_quotes"):
            print(f"[DEBUG] Stripping quotes from compare values in field '{field_name}'")
            logger.debug(f"[REPLACER] Rule requests strip_quotes=True for field '{field_name}' — applying to compare_val and match_target")
            compare_val = compare_val.strip('"').strip("'")
            match_target = match_target.strip('"').strip("'")

        # Apply case sensitivity
        if not case_sensitive:
            compare_val = compare_val.lower()
            match_target = match_target.lower()

        # Apply replacement
        if match_type == "exact" and compare_val == match_target:
            updated_row[col_idx] = new_value
            was_changed = True
            if log_fn:
                log_fn(f"[REPLACED] Field '{field_name}': '{current_value}' → '{new_value}'")

        elif match_type == "substring" and match_target in compare_val:
            new_text = (
                current_value.replace(old_value, new_value)
                if case_sensitive
                else _replace_insensitive(current_value, old_value, new_value)
            )
            if new_text != current_value:
                updated_row[col_idx] = new_text
                was_changed = True
                if log_fn:
                    log_fn(f"[REPLACED] Field '{field_name}': '{current_value}' → '{new_text}'")

    return updated_row, was_changed


def _replace_insensitive(text, old, new):
    """
    Helper for case-insensitive substring replacement.
    """
    import re
    pattern = re.compile( re.escape( old ), re.IGNORECASE )
    return pattern.sub( new, text )
