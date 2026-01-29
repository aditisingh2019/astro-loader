"""
Configuration loader and environment resolver.

Responsibilities:
- Load base configuration from config/base.yaml.
- Load environment-specific overrides (e.g., local.yaml, dev.yaml).
- Merge configurations with correct precedence:
    environment variables > env YAML > base YAML.
- Validate presence of required configuration values.
- Expose a unified, read-only configuration object to the application.

Important Behavior:
- Secrets (DB credentials, tokens) must NEVER be read from YAML files.
- Environment variables always override YAML values.
- Missing required config should raise a clear, actionable error.
- Configuration loading must be deterministic and testable.
"""
