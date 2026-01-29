"""
Schema validation definitions.

Responsibilities:
- Define the expected structure of incoming records.
- Enforce required fields, data types, and nullability.
- Provide clear, structured validation error messages.

Important Behavior:
- Schema validation occurs before any transformation or cleaning.
- A single record may fail multiple schema checks.
- Validation errors must be deterministic and explainable.
- Schema definitions should be easy to extend or version.

This layer ensures:
- Only structurally sound data proceeds to transformation.
"""
