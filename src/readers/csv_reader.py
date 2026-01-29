"""
CSV file reader implementation.

Responsibilities:
- Read CSV files from local disk (or mounted paths).
- Parse rows into dictionaries keyed by column names.
- Handle CSV-specific concerns (delimiters, headers, encoding).

Important Behavior:
- Must stream rows to avoid loading large files into memory.
- Must not perform schema validation or business rule checks.
- Should surface malformed rows as reader-level exceptions.
- Should support configurable CSV options via config files.
"""
