"""
Purpose:
--------
Orchestrate the end-to-end data ingestion workflow.

Responsibilities:
-----------------
- Coordinate reading, validation, transformation, deduplication, and loading.
- Handle pipeline-level errors.
- Emit summary logs for each run.

Important Behavior:
-------------------
- Executes steps in a strict, predictable order.
- Ensures bad data does not block good data.
- Serves as the future entry point for schedulers (Airflow).

Design Notes:
-------------
- Contains no business logic.
- Optimized for readability and debuggability.
"""
