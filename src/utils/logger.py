"""
Purpose:
--------
Provide a centralized logging utility for the ingestion pipeline.

Responsibilities:
-----------------
- Configure log format and log level.
- Provide a reusable logger instance to all modules.
- Ensure consistent, structured logging across the application.

Important Behavior:
-------------------
- Logs must include timestamp, severity, and module name.
- Logging should never crash the pipeline.
- Used for operational visibility and debugging.

Design Notes:
-------------
- Uses Python's built-in logging library.
- Designed to be easily extended for cloud logging (CloudWatch, Datadog).
"""
