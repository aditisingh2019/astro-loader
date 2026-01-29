"""
Purpose:
--------
Load processed data into PostgreSQL staging tables.

Responsibilities:
-----------------
- Insert valid records into staging tables.
- Insert rejected records into reject tables.
- Manage database transactions.

Important Behavior:
-------------------
- Uses batch inserts for performance.
- Commits only after successful loads.
- Rolls back on database-level errors.

Design Notes:
-------------
- Separates valid and invalid load paths.
- Designed to be reusable across datasets.
"""
