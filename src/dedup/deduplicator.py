"""
Record deduplication logic.

Responsibilities:
- Identify duplicate records using a defined strategy.
- Remove duplicates from the valid record set.
- Optionally emit duplicates as rejected records.

Important Behavior:
- Deduplication strategy must be explicit:
    - natural key
    - hash of canonicalized record
- Deduplication must be deterministic across runs.
- Should support idempotent ingestion (safe re-runs).
- Duplicate handling behavior must be configurable.

This layer protects downstream systems from double-counting.
"""
