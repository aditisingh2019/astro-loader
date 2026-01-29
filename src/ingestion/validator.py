"""
Purpose:
--------
Validate incoming data against structural and business rules.

Responsibilities:
-----------------
- Verify required columns exist.
- Validate data types and null constraints.
- Identify invalid records without stopping the pipeline.

Important Behavior:
-------------------
- Splits data into valid and rejected records.
- Adds a rejection reason to invalid records.
- Does not mutate valid data values.

Design Notes:
-------------
- Validation rules are config-driven.
- Designed to support schema evolution.
"""
