"""
PostgreSQL connection management.

Responsibilities:
- Create and manage database connections.
- Read connection parameters from environment variables.
- Provide safe connection lifecycle handling.

Important Behavior:
- Connections must be explicitly closed after use.
- Connection creation should be isolated from business logic.
- Fail fast if required credentials are missing.
- Should support future connection pooling if needed.
"""
