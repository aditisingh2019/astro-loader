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
from logging import handlers
import atexit
from queue import Queue
from pythonjsonlogger import jsonlogger
from src.utils.db_log_handler import DatabaseLogHandler
from src.db import connection

def setup_logger():

    # Create logging instance.
    logger = logging.getLogger()

    # Check if logger has handlers to avoid creating multiple.
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    engine = connection.get_engine()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s | %(lineno)s | %(message)s")
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler
    # file_handler = logging.FileHandler("logs/astro-loader.log")
    # file_handler.setLevel(logging.INFO)
    file_format = '%(asctime)s %(levelname)s %(module)s %(lineno)s %(message)s'
    # json_file_format = jsonlogger.JsonFormatter(file_format)
    # file_handler.setFormatter(json_file_format)
    # logger.addHandler(file_handler)
 
    # Create queue and quehandler for database log inserts
    queue = Queue(maxsize=1000)
    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)
    logger.addHandler(queue_handler)

    # Database log handler
    db_handler = DatabaseLogHandler(engine)
    db_handler.setLevel(logging.INFO)
    db_handler.setFormatter(file_format)

    queue_listener = logging.handlers.QueueListener(queue, db_handler, respect_handler_level=True)
    queue_listener.start()

    # Stop queue listener when application ends
    atexit.register(queue_listener.stop)
    
    return logger
