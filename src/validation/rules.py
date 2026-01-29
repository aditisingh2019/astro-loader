"""
Business rule validation logic.

Responsibilities:
- Enforce domain-specific data quality rules.
- Perform cross-field and conditional validations.
- Produce structured rejection reasons for invalid records.

Important Behavior:
- Rules should be independent and composable.
- Rule failures must not raise exceptions.
- Each failure must include:
    - rule name
    - human-readable reason
- Valid records continue downstream even if other records fail.

This layer captures business correctness, not structural validity.
"""
