"""
SQL query definitions.

Responsibilities:
- Store parameterized SQL statements used by the application.
- Centralize table definitions and insert logic.
- Avoid SQL scattered across the codebase.

Important Behavior:
- All queries must be parameterized to prevent SQL injection.
- Queries should be readable and well-documented.
- Table assumptions (constraints, indexes) should be documented here.

This file acts as the contract between Python and PostgreSQL.
"""
