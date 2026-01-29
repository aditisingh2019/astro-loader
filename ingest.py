"""
Entry point for the Data Ingestion Subsystem.

Responsibilities:
- Acts as the orchestration layer for a single ingestion run.
- Parses CLI arguments (e.g., source type, file path, environment).
- Loads configuration and environment variables.
- Initializes logging and database connections.
- Coordinates the end-to-end ingestion workflow:
    read → validate → clean → deduplicate → load.
- Ensures proper shutdown of resources (DB connections, file handles).

Important Behavior:
- This file should contain minimal business logic.
- Fail fast on configuration, source, or infrastructure errors.
- Data-level errors must not stop ingestion; they are handled downstream.
- Emits a final ingestion summary (counts, duration, status).
"""
