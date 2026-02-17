"""
Purpose:
--------
Call a database procedure.

Responsibilities:
-----------------
- 
- 
- 

Important Behavior:
-------------------
- 
- 
- 

Design Notes:
-------------
- 

"""

import logging
from sqlalchemy import text
from src.db.connection import get_engine

logger = logging.getLogger(__name__)

def call_procedure(procecure_name : str=None, proceure_params : dict=None):

    engine = get_engine()

    try:
        with engine.connect() as connection:

            if procecure_name is None:
                connection.execute(text("CALL transfer_uber_data();"))
                connection.commit()
            else:
                parameters = {}
                parameters = ", ".join(f":{key}" for key in proceure_params.keys())
                query = f"CALL {procecure_name}(:parameters);"
                connection.exececute(text(procecure_name), proceure_params)
                connection.commit()

        logger.info("Database procedure was called successfully")

    except Exception as e:
        logger.error("Failed to execute database procedure ")
        raise
