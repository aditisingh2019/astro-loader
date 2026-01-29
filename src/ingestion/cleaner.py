"""
Purpose:
--------
Clean and standardize validated data before loading.

Responsibilities:
-----------------
- Normalize column names.
- Standardize data formats (dates, strings, numbers).
- Handle missing values where appropriate.

Important Behavior:
-------------------
- Only operates on valid records.
- Should not introduce new invalid states.
- Keeps transformations predictable and reversible.

Design Notes:
-------------
- Focused on light transformations, not business logic.
- Designed to be composable and testable.
"""
