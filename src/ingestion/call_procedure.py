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
from sqlalchemy import text, Engine

logger = logging.getLogger(__name__)

def call_procedure(engine: Engine, procedure_name : str=None, procedure_params : dict=None):

    try:
        with engine.begin() as connection:

            if procedure_name is None:
                connection.execute(text("CALL transfer_uber_data();"))
            else:
                if procedure_params:
                    parameters = {}
                    parameters = ", ".join(f":{key}" for key in procedure_params.keys())
                    query = f"CALL {procedure_name}({parameters});"
                    connection.execute(text(query), procedure_params)
                else:
                    query = f"CALL {procedure_name}();"
                    connection.execute(text(query))

        logger.info("Database procedure was called successfully")

    except Exception as e:
        logger.error("Failed to execute database procedure ")
        raise
