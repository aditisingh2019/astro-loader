"""
Manage PostgreSQL database connections for the ingestion pipeline.
- Create and return database connections.
- Handle connection configuration securely.
- Ensure connections are properly closed after use.

"""

import os
import logging
from sqlalchemy import create_engine, URL, text
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
BASE_SQL_PATH = Path(__file__).parents[2] / "sql"

def get_engine():

    # Load the .env data
    load_dotenv()

    # Assign the .env data to variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_host, db_port, db_name, db_user, db_password]):
        raise EnvironmentError("Database environment variables are not fully set")

    # Create a url from the above variables
    db_url = URL.create(
        drivername="postgresql+psycopg2",
        username=db_user,
        password=db_password,
        host=db_host,
        port=int(db_port),
        database=db_name
    )

    # Create an engine for dabase connections
    try:
        engine = create_engine(db_url, pool_size=5, max_overflow=10, pool_pre_ping=True, future=True)
        return engine
    except Exception as e:
        logger.exception(f"An error occurred while creating the engine: {e}")
        raise e
        
def execute_sql_file(engine: Engine, sql_path: Path) -> None:
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text()

    with engine.begin() as conn:
        conn.execute(text(sql))


def setup_database() -> None:
    engine = get_engine()

    execute_sql_file(engine, BASE_SQL_PATH / "schema" / "staging.sql")
    execute_sql_file(engine, BASE_SQL_PATH / "schema" / "core.sql")
    execute_sql_file(engine, BASE_SQL_PATH / "procedures" / "transfer_stage_to_core.sql")