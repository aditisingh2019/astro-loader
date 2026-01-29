"""
Purpose:
--------
Manage PostgreSQL database connections for the ingestion pipeline.

Responsibilities:
-----------------
- Create and return database connections.
- Handle connection configuration securely.
- Ensure connections are properly closed after use.

Important Behavior:
-------------------
- Uses parameterized queries to prevent SQL injection.
- Does not contain business logic.
- Raises connection-level errors clearly and early.

Design Notes:
-------------
- Designed to be reusable across loaders.
- Compatible with AWS RDS PostgreSQL.
"""
