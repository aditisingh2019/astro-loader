"""
Database loading logic.

Responsibilities:
- Insert validated records into staging tables.
- Insert rejected records into the reject table.
- Manage transactions and commit/rollback behavior.

Important Behavior:
- Valid and rejected records should be loaded atomically per batch.
- A failure during loading must trigger a rollback.
- Should support batch inserts for performance.
- Must not silently swallow database errors.

This layer is responsible for data durability and correctness.
"""
