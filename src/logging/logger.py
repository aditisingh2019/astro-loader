"""
Application logging configuration.

Responsibilities:
- Configure structured logging (level, format, handlers).
- Attach contextual metadata (batch_id, source_file).
- Ensure consistent logging across modules.

Important Behavior:
- Logging verbosity must be environment-dependent.
- Logs must clearly distinguish:
    INFO (normal flow)
    WARNING (data issues)
    ERROR (system failures)
- Logging should be machine-readable where possible.

Good logs are essential for operating data systems.
"""
