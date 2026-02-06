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

import logging
import yaml
import logging.config

def setup_logging(config_file="config/config.yaml"):

    with open(config_file, 'rt') as file:
        try:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config["logging"])
        except Exception:
            logging.basicConfig(level=logging.INFO)