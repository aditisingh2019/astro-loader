"""
Purpose:
--------
- Handle logging persistance.

Responsibilities:
-----------------
- Initialize a database logging handler along with a database connection.
- Execute log insertions to respective database tables.

Important Behavior:
-------------------
- The logging level determines what table the logs are inserted into.
- Works in conjunction with the QueueHandler and QueueListener established at setup.
- Can be modified to handle batch insertions.

Design Notes:
-------------
- Simplified for handling both error log and data ingestion log.
"""

import logging
from sqlalchemy import insert
from datetime import datetime
from src.db.tables import error_log_table, data_log_table

class DatabaseLogHandler(logging.Handler):

    def __init__(self, engine):
        super().__init__()
        self.engine = engine
        self.error_log_table = error_log_table
        self.data_log_table = data_log_table

    def emit(self, record):

        try:
            row = {
                "asctime" : datetime.fromtimestamp(record.created),
                "levelname" : record.levelname,
                "module" : record.module,
                "lineno" : record.lineno,
                "message" : record.getMessage()
            }

            table = self.error_log_table if record.levelno >= logging.ERROR else self.data_log_table

            with self.engine.begin() as connection:
                query = insert(table).values(**row)
                connection.execute(query)

        except Exception as e:
            self.handleError(record)

