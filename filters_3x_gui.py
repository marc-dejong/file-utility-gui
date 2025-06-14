"""
3x_Filters.py
Applies filter rules from shared_data to input_data.
Supports match types: equals, contains, starts_with
Handles case sensitivity and quote stripping as per flags
"""

from datetime import datetime

def apply_filters(row, filter_rules, flags, header=None):
    match = True

    for rule in filter_rules:
        field = rule["field"]
        is_date_rule = rule.get("filter_subtype") == "date"
        values = rule.get("value", [])

        # For text filters only
        if not is_date_rule:
            match_type = rule.get("match_type")

        # Resolve index from field name or number
        try:
            if field.isdigit():
                field_index = int(field)
            elif header is not None and field in header:
                field_index = header.index(field)
            else:
                raise ValueError(f"Field '{field}' not recognized as index or header name.")

            field_value = row[field_index]
        except (ValueError, IndexError) as e:
            print(f"[WARN] Skipping row due to bad field reference: {e}")
            match = False
            break

        # Normalize value (only for non-date, non-numeric filters)
        if not is_date_rule and rule.get( "match_type" ) != "numeric":
            if flags.get( "strip_quotes" ):
                field_value = field_value.strip().strip( '"' ).strip( "'" )
                values = [v.strip().strip( '"' ).strip( "'" ) for v in values]

            if not flags.get( "case_sensitive" ):
                field_value = field_value.lower()
                values = [v.lower() for v in values]

        # Apply rule
        if is_date_rule:
            try:
                field_dt = datetime.strptime(field_value.strip(), rule["format_code"])
            except Exception as e:
                print(f"[WARN] Could not parse date in field '{field}': {field_value} — {e}")
                match = False
                break

            if rule["filter_type"] == "exact":
                if field_dt.date() != rule["start_date"].date():
                    match = False
                    break
            elif rule["filter_type"] == "before":
                if field_dt > rule["start_date"]:
                    match = False
                    break
            elif rule["filter_type"] == "after":
                if field_dt < rule["start_date"]:
                    match = False
                    break
            elif rule["filter_type"] == "range":
                if not (rule["start_date"] <= field_dt <= rule["end_date"]):
                    match = False
                    break
            else:
                print(f"[WARN] Unknown date filter_type: {rule['filter_type']}")
                match = False
                break
        else:
            if match_type == "numeric":
                try:
                    field_number = float(field_value)
                except ValueError:
                    print(f"[WARN] Non-numeric value in numeric filter field '{field}': {field_value}")
                    match = False
                    break

                operator = rule.get("operator")
                if operator == "equals":
                    if field_number != values[0]:
                        match = False
                        break
                elif operator == "greater_than":
                    if field_number <= values[0]:
                        match = False
                        break
                elif operator == "greater_than_or_equal":
                    if field_number < values[0]:
                        match = False
                        break
                elif operator == "less_than":
                    if field_number >= values[0]:
                        match = False
                        break
                elif operator == "less_than_or_equal":
                    if field_number > values[0]:
                        match = False
                        break
                elif operator == "between":
                    if not (values[0] <= field_number <= values[1]):
                        match = False
                        break
                else:
                    print(f"[WARN] Unknown numeric operator: {operator}")
                    match = False
                    break

            elif match_type == "equals":
                if field_value not in values:
                    match = False
                    break
            elif match_type == "contains":
                if not any(v in field_value for v in values):
                    match = False
                    break
            elif match_type == "starts_with":
                if not any(field_value.startswith(v) for v in values):
                    match = False
                    break
            else:
                match = False
                break


    return match

