"""
Purpose:
--------
Read raw data from external file sources into memory.

Responsibilities:
-----------------
- Support reading CSV and JSON files.
- Return data as a Pandas DataFrame.
- Handle file-level errors gracefully.

Important Behavior:
-------------------
- File type selection is config-driven.
- Does not perform validation or cleaning.
- Raises clear errors for unreadable files.

Design Notes:
-------------
- Designed for easy extension to new formats (e.g., Parquet).
- Keeps I/O logic isolated from business logic.
"""
