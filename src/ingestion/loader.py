"""
Purpose:
--------
Load processed data into PostgreSQL staging tables.

Responsibilities:
-----------------
- Insert valid records into staging tables.
- Insert rejected records into reject tables.
- Manage database transactions.

Important Behavior:
-------------------
- Uses batch inserts for performance.
- Commits only after successful loads.
- Rolls back on database-level errors.

Design Notes:
-------------
- Separates valid and invalid load paths.
- Designed to be reusable across datasets.
"""

from __future__ import annotations

import logging
import json
from matplotlib import table
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# Load valid and rejected records into database.
def load_data(
    engine: Engine,
    valid_df: pd.DataFrame,
    reject_df: pd.DataFrame,
    staging_table,
    reject_table,
    batch_size: int = 5000
) -> None:


    if valid_df.empty and reject_df.empty:
        logger.info("No records to load.")
        return

    logger.info(
        f"Starting load. Valid rows: {len(valid_df)}, "
        f"Rejected rows: {len(reject_df)}"
    )

    try:
        with engine.begin() as connection:

            if not valid_df.empty:
                _batch_insert(
                    connection,
                    staging_table,
                    valid_df,
                    batch_size=batch_size
                )
                logger.info(f"Inserted {len(valid_df)} valid rows.")

            if not reject_df.empty:
                prepared_rejects = _prepare_reject_records(reject_df)

                _batch_insert(
                    connection,
                    reject_table,
                    prepared_rejects,
                    batch_size=batch_size
                )
                logger.info(f"Inserted {len(reject_df)} rejected rows.")

        logger.info("Load committed successfully.")

    except Exception as e:
        logger.exception("Database load failed. Transaction rolled back.")
        raise



# Helpers
def _batch_insert(
    connection,
    table,
    df: pd.DataFrame,
    batch_size: int
):
    # Perform batch inserts using SQLAlchemy Core.

    records = df.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        stmt = insert(table)
        connection.execute(stmt, batch)

# Convert rejected dataframe into schema expected by reject table.
def _prepare_reject_records(reject_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparing rejected records for reject table.")

    reject_copy = reject_df.copy()

    reject_copy["raw_record"] = reject_copy.apply(
        lambda row: json.dumps(row.to_dict()),
        axis=1
    )

    prepared = pd.DataFrame({
        "source_name": "ride_bookings",
        "raw_record": reject_copy["raw_record"],
        "reject_reason": reject_copy["reject_reason"]
    })

    return prepared
