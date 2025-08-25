import os
import csv
import datetime

class NormalizeLogger:
    def __init__(self, output_dir="logs", log_name_prefix="normalize_log"):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = output_dir
        self.log_filename = f"{log_name_prefix}_{timestamp}.csv"
        self.log_path = os.path.join(self.output_dir, self.log_filename)
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize the log file
        with open(self.log_path, mode="w", newline="", encoding="utf-8") as logfile:
            writer = csv.writer(logfile)
            writer.writerow(["RecordIndex", "OriginalValue", "NormalizedValue", "FieldIndex", "Status"])

    def log_change(self, record_index, field_index, original, normalized, status="OK"):
        with open(self.log_path, mode="a", newline="", encoding="utf-8") as logfile:
            writer = csv.writer(logfile)
            writer.writerow([record_index, original, normalized, field_index, status])

    def log_error(self, record_index, field_index, original, error_msg):
        self.log_change(record_index, field_index, original, "", f"ERROR: {error_msg}")

    def get_log_path(self):
        return self.log_path