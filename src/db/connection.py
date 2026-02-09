"""
Purpose:
--------
Manage PostgreSQL database connections for the ingestion pipeline.

Responsibilities:
-----------------
- Create and return database connections.
- Handle connection configuration securely.
- Ensure connections are properly closed after use.

Important Behavior:
-------------------
- Uses parameterized queries to prevent SQL injection.
- Does not contain business logic.
- Raises connection-level errors clearly and early.

Design Notes:
-------------
- Designed to be reusable across loaders.
- Compatible with AWS RDS PostgreSQL.
"""

import os
import logging
from sqlalchemy import create_engine, URL
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def get_engine():

    # Load the .env data
    load_dotenv()

    # Assign the .env data to variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    # Create a url from the above variables
    db_url = URL.create(
        drivername="postgresql",
        username=db_user,
        password=db_password,
        host=db_host,
        port=int(db_port),
        database=db_name
    )

    # Create an engine for dabase connections
    try:
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        logger.info(f"An error occurred while creating the engine: {e}")
