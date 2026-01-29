"""
Internal data record model.

Responsibilities:
- Define the canonical in-memory representation of a data record.
- Encapsulate metadata such as:
    - batch_id
    - source_file
    - ingestion timestamp
- Provide a consistent structure across pipeline stages.

Important Behavior:
- Models should be lightweight and immutable where possible.
- Should support serialization for database insertion.
- Must not include database-specific logic.

This model decouples ingestion logic from raw source formats.
"""
