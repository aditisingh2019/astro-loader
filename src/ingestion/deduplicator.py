"""
Purpose:
--------
Remove duplicate records from the dataset.

Responsibilities:
-----------------
- Identify duplicates based on configured keys.
- Remove duplicate rows deterministically.
- Preserve the first or latest record based on rules.

Important Behavior:
-------------------
- Deduplication logic is config-driven.
- Runs after cleaning and before loading.
- Does not affect rejected records.

Design Notes:
-------------
- Supports single or composite keys.
- Designed for performance on medium-sized datasets.
"""
