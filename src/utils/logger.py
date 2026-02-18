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
import os
from dotenv import load_dotenv
from queue import Queue
from src.utils.db_log_handler import DatabaseLogHandler
from sqlalchemy.engine import Engine

def setup_logger(engine: Engine):
    
    load_dotenv()

    # Create logging instance.
    logger = logging.getLogger()

    # Check if logger has handlers to avoid creating multiple.
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s | %(lineno)s | %(message)s")
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # Format for database logging
    db_format = '%(asctime)s %(levelname)s %(module)s %(lineno)s %(message)s'
 
    # Create queue and quehandler for database log inserts
    queue = Queue(maxsize=1000)
    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)
    logger.addHandler(queue_handler)

    # Database log handler
    db_handler = DatabaseLogHandler(engine)
    db_handler.setLevel(logging.INFO)
    db_handler.setFormatter(db_format)

    # Email log handler
    email_host = os.getenv("EMAIL_HOST")
    email_port = os.getenv("EMAIL_PORT")

    if email_host and email_port: 
        smtp_handler = handlers.SMTPHandler(
            mailhost=(email_host, int(email_port)),
            fromaddr=os.getenv("EMAIL_ADDRESS"),
            toaddrs=[os.getenv("EMAIL_RECIPIENT")],
            subject="Data ingestion log update",
            credentials=(os.getenv("EMAIL_ADDRESS"), os.getenv("EMAIL_PASSWORD")),
            secure=()
        )
        smtp_handler.setLevel(logging.CRITICAL)
        smtp_handler.setFormatter(console_format)
        logger.addHandler(smtp_handler)

    queue_listener = logging.handlers.QueueListener(queue, db_handler, respect_handler_level=True)
    queue_listener.start()

    # Stop queue listener when application ends
    atexit.register(queue_listener.stop)

    return logger
