"""
dev_test_12x_streaming_normalizer.py

Standalone test harness for normalize_streaming_record() from normalizer_12x.py.
Used to validate core normalization logic with a hardcoded record and config.

Author: Marc DeJong
Created: 2025-07-16
"""

from normalizer_12x import normalize_streaming_record

def main():
    # Sample test record (simulated CSV row)
    test_record = ["123", "45.678", "(89.00)", "  abc ", "$1,234.50"]

    # Config to simulate field metadata
    config = {
        "col_list": [0, 1, 2, 3, 4],
        "field_names": ["ID", "Value1", "Value2", "Text", "Currency"],
        "sample_records": [
            ["001", "10.5", "100.00", "xyz", "$999.99"],
            ["002", "20.0", "(50.00)", "abc", "$123.45"]
        ],
        "source_file_path": "sample_input.csv",
        "record_index": 1
    }


    normalized = normalize_streaming_record(test_record, config)
    print("Original Record:  ", test_record)
    print("Normalized Output:", normalized)

if __name__ == "__main__":
    main()