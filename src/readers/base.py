"""
Abstract base class for data source readers.

Responsibilities:
- Define a common interface for all data readers.
- Enforce a contract for reading records from a source.
- Ensure readers return records in a consistent dictionary format.

Important Behavior:
- Readers should stream data (iterator/generator), not load entire files.
- Readers should not perform validation or transformation.
- Any source-specific parsing errors should raise explicit exceptions.

This abstraction allows:
- Adding new source types (Parquet, API, S3, Kafka) without changing
  downstream ingestion logic.
"""
