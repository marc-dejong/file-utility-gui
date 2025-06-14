# 4x_Replacer.py

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

        # Optionally strip surrounding quotes
        if replacement_config.get( "strip_quotes" ):
            current_value = current_value.strip( '"' ).strip( "'" )

        # Optionally trim whitespace
        current_value = current_value.strip()

        compare_val = current_value if case_sensitive else current_value.lower()

        print(
            f"[CHECK] Row value='{current_value}' | Comparing to='{old_value}' | Field='{field_name}' | Case sensitive={case_sensitive}" )

        match_target = old_value if case_sensitive else old_value.lower()

        if match_type == "exact" and compare_val == match_target:
            updated_row[col_idx] = new_value
            was_changed = True
            if log_fn:
                log_fn( f"[REPLACED] Field '{field_name}': '{current_value}' → '{new_value}'" )

        elif match_type == "substring" and match_target in compare_val:
            new_text = (current_value.replace( old_value, new_value )
                        if case_sensitive else _replace_insensitive( current_value, old_value, new_value ))
            if new_text != current_value:
                updated_row[col_idx] = new_text
                was_changed = True
                if log_fn:
                    log_fn( f"[REPLACED] Field '{field_name}': '{current_value}' → '{new_text}'" )

    return updated_row, was_changed


def _replace_insensitive(text, old, new):
    """
    Helper for case-insensitive substring replacement.
    """
    import re
    pattern = re.compile( re.escape( old ), re.IGNORECASE )
    return pattern.sub( new, text )
