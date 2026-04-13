# Error Handling for ETL


# Strategy 1: Fail Fast — Stop for Critical Errors

# import os
import logging


# def get_required_env(name:  str) -> str:
#     """Get an environment variable, or fail immediately if missing."""
#     value = os.environ.get(name)
#     if value is None:
#         raise EnvironmentError(
#             f"Required environment variable '{name}' is not set. "
#             f"Set it before running the pipeline."
#         )
#     return value


# db_host = get_required_env("DB_HOST")
# db_port = get_required_env("DB_PORT")
# db_key = get_required_env("DB_KEY")


# print(f"Connecting to database at {db_host}:{db_port} with key {db_key}...")



# Write a function called validate_file_exists(path) that:

# Checks if the file at path exists (hint: from pathlib import Path; Path(path).exists())
# If not, raises a FileNotFoundError with a message including the path
# If it exists but is empty (size 0), raises a ValueError saying the file is empty
# Returns the path if everything is OK


# def validate_file_exists(path: str) -> str:
#     """Check if the file at path exists and is not empty."""
#     from pathlib import Path
#     file = Path(path)
#     if not file.exists():
#         raise FileExistsError(f"File not found: {path}")
#     if file.stat().st_size == 0:
#         raise ValueError(f"File is empty: {path}")
#     return path


# print(validate_file_exists("data/raw/orders_2026.csv"))  # should return the path
# print(validate_file_exists("data/raw/notes.txt"))  # should raise FileNotFoundError




# Strategy 2: Log and Skip — Handle Bad Records



# Set up logging — messages go to the console AND a file
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("pipeline")



# def transform_batch(records: list[dict]) -> list[dict]:
#     """Transform records, logging and skipping bad onces."""
#     results = []
#     error_count = 0

#     for i, record in enumerate(records):
#         try:
#             # Attempt to transform this record
#             cleaned = {
#                 "id": int(record["id"]),
#                 "amount": float(record["amount"]),
#                 "name": record["name"].strip().upper(),
#             }
#             results.append(cleaned)

#         except (ValueError, KeyError, TypeError) as e:
#             # This specific record is bad  - log it and move on
#             error_count += 1
#             logger.warning(f"Skipping record {i}: {e} | raw={record}")


    # logger.info(f"Transformed  {len(results)} records, skipped {error_count} bad records.")
    # return results



# records = [
#     {"id": "1", "amount": "150.00", "name": "Alice"},     # ✓ good
#     {"id": "2", "amount": "not_a_number", "name": "Bob"},  # ✗ amount isn't a number
#     {"id": "three", "amount": "300", "name": "Carol"},     # ✗ id isn't a number
#     {"id": "4", "amount": "200.00", "name": "Dave"},       # ✓ good
# ]

# result = transform_batch(records)



# Write a function parse_dates(date_strings) that:

# Takes a list of strings like ["2026-04-10", "not-a-date", "2026-01-15", "", "2026-12-31"]
# Tries to parse each one with datetime.strptime(s, "%Y-%m-%d")
# Logs and skips bad ones
# Returns a list of successfully parsed datetime objects



# def parse_dates(date_strings: list[str]) -> list:
#     """Parse a list of date strings, logging and skipping bad ones."""
#     from datetime import datetime
#     parsed_dates = []
#     error_count = 0

#     for s in date_strings:
#         try:
#             dt = datetime.strptime(s, "%Y-%m-%d")
#             parsed_dates.append(dt)
#         except ValueError as e:
#             error_count += 1
#             logging.warning(f"Skipping invalid date string: '{s}' | error: {e}")

#     logging.info(f"Parsed {len(parsed_dates)} dates, skipped {error_count} bad strings.")
#     return parsed_dates

# date_strings = ["2026-04-10", "not-a-date", "2026-01-15", "", "2026-12-31"]
# parsed = parse_dates(date_strings)
# print(parsed)  # should print the three valid datetime objects




# Strategy 3: Collect and Report — Data Quality Monitoring
from dataclasses import dataclass, field

@dataclass
class DataQualityReport:
    """Collects data quality checks and produces a summary."""
    total_checks: int = 0
    passed: int = 0
    issues: list[dict] = field(default_factory=list)

    @property
    def failed(self) -> int:
        return self.total_check - self.passed

    @property
    def error_rate(self) -> float:
        if self.total_check ==0:
            return 0.0
        return self.failed / self.total_checks


    def check(self, record: dict, condition: bool, rule_name: str):
        """Run one quality check on a record."""
        self.total_check += 1
        if condition:
            self.passed += 1
        else:
            self.issues.append({
                "record": record,
                "rule": rule_name,
            })



report = QualityReport()   


for row in data:
    report.check(row, row.get("id") is not None, "id_not_null")
    report.check(row, row.get("amount", 0) >= 0, "amount_non_negative")
    report.check(row, row.get("email", "").count("@") == 1, "vaild_email")


# After processing all rows, summarize  
print(f"Quality: {report.passed}/{report.total_checks} passed")
print(f"Error rate: {report.error_rate:.1%}")
print(f"Issues found: {report.failed}")


# Fail the pipeline if quality is too low
if report.error_rate > 0.05:
    raise ValueError(
        f"Data quality too low: {report.error_rate:.1%} error rate, "
        f"exceeds 5% threshold. See {report.failed} issues."
    )