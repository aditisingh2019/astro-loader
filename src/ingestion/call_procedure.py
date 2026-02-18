"""
Purpose:
--------
Call a database procedure.

Responsibilities:
-----------------
- Execute a procedure in the database.
- Handles different procedures with or without parameters.

Important Behavior:
-------------------
- Can specify a default procedure call.
- Accepts procedure name as string and parameters in a dictionary.

Design Notes:
-------------
- The default procedure call is currently specific to the Uber dataset.

"""

import logging
from pathlib import Path
from sqlalchemy import text, Engine
from src.db.connection import get_engine, execute_sql_file

logger = logging.getLogger(__name__)

def call_procedure(engine=None) -> None:

    if engine is None:
        engine = get_engine()

    sql_path = Path(__file__).parents[2] / "sql" / "procedures" / "transfer_stage_to_core.sql"

    # Ensure procedure is deployed
    execute_sql_file(engine, sql_path)

    with engine.begin() as conn:
        conn.execute(text("CALL transfer_stage_to_core();"))
