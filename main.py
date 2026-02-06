"""
Purpose:
--------
Application entry point for the data ingestion subsystem.

Responsibilities:
-----------------
- Load configuration.
- Initialize logging.
- Trigger pipeline execution.

Important Behavior:
-------------------
- Minimal logic by design.
- Exits with meaningful status codes.
- Suitable for CLI or scheduled execution.

Design Notes:
-------------
- Keeps runtime concerns separate from pipeline logic.
"""

import logging
from src.utils import logger

# Initialize logging
logger.setup_logging()
log = logging.getLogger(__name__)
