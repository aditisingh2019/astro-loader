"""
Data cleaning and standardization logic.

Responsibilities:
- Normalize field names, formats, and casing.
- Standardize date, numeric, and categorical values.
- Apply default values where appropriate.

Important Behavior:
- Transformations apply ONLY to validated records.
- No record should be dropped in this layer.
- Cleaning logic must be deterministic and reversible where possible.
- Assumptions and defaults must be documented.

This layer prepares data for analytical consistency.
"""
