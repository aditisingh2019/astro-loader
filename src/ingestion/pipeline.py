"""
Purpose:
--------
Orchestrate the end-to-end data ingestion workflow.

Responsibilities:
-----------------
- Coordinate reading, validation, transformation, deduplication, and loading.
- Handle pipeline-level errors.
- Emit summary logs for each run.

Important Behavior:
-------------------
- Executes steps in a strict, predictable order.
- Ensures bad data does not block good data.
- Serves as the future entry point for schedulers (Airflow).

Design Notes:
-------------
- Contains no business logic.
- Optimized for readability and debuggability.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from src.db.connection import get_engine
from src.db.tables import metadata
from src.ingestion.reader import read_file
from src.ingestion.validator import validate_dataframe
from src.ingestion.cleaner import clean_dataframe
from src.ingestion.deduplicator import deduplicate
from src.ingestion.loader import load_data
from src.ingestion.call_procedure import call_procedure
from src.db.tables import stg_rides_table, stg_rejects_table
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Pipeline Entry Point. Execute full ingestion pipeline.
def run_pipeline(
    filename: str,
    chunksize: int = 10000,
    engine= Engine
) -> None:

    start_time = time.time()

    logger.info(f"Starting ingestion pipeline for file: {filename}")

    engine = get_engine()

    total_rows = 0
    total_valid = 0
    total_rejected = 0
    total_deduped = 0
    viewed_records = set()

    try:
        reader = read_file(filename, chunksize)

        for chunk_number, chunk in enumerate(reader, start=1):

            logger.info(f"Processing chunk {chunk_number} (rows={len(chunk)})")
            total_rows += len(chunk)

            # Validation
            valid_df, reject_df = validate_dataframe(chunk)

            total_rejected += len(reject_df)

            # Cleaning
            cleaned_valid_df = clean_dataframe(valid_df)

            # Deduplication
            deduped_df = deduplicate(cleaned_valid_df, viewed_records)

            deduped_count = len(cleaned_valid_df) - len(deduped_df)
            total_deduped += deduped_count

            total_valid += len(deduped_df)

            # Load
            load_data(
                engine=engine,
                valid_df=deduped_df,
                reject_df=reject_df,
                staging_table=stg_rides_table,
                reject_table=stg_rejects_table
            )

            logger.info(
                f"Chunk {chunk_number} complete | "
                f"Valid: {len(deduped_df)} | "
                f"Rejected: {len(reject_df)} | "
                f"Deduplicated: {deduped_count}"
            )

        
        # Transfer uploaded data from staging table to actual tables
        call_procedure(engine=engine)
        logger.info("All chunks processed. Calling transfer procedure.")
        runtime = round(time.time() - start_time, 2)

        logger.info(
            "Pipeline execution complete | "
            f"Total rows: {total_rows} | "
            f"Total valid loaded: {total_valid} | "
            f"Total rejected: {total_rejected} | "
            f"Total deduplicated: {total_deduped} | "
            f"Runtime: {runtime}s"
        )
        
        # Testing the SMTPHandler
        logger.critical("Test for SMTPHandler.")

    except Exception as e:
        logger.exception("Pipeline execution failed.")
        raise

    finally:
        logger.info("Pipeline finished.")
