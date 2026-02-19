"""
Purpose:
--------
Load processed data into PostgreSQL staging tables.

Responsibilities:
-----------------
- Insert valid records into staging tables.
- Insert rejected records into reject tables.
- Manage database transactions safely.

Important Behavior:
-------------------
- Uses batch inserts for performance.
- Commits only after successful loads.
- Rolls back automatically on database errors.
- Does NOT contain business logic or transformations.

Design Notes:
-------------
- Generic and reusable across datasets.
- Assumes data is already validated and cleaned.
"""

from __future__ import annotations

import logging
import json
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def load_data(
    engine: Engine,
    valid_df: pd.DataFrame,
    reject_df: pd.DataFrame,
    staging_table,
    reject_table,
    batch_size: int = 5000
) -> None:
    """
    Load valid and rejected records into database.

    Parameters
    ----------
    engine : SQLAlchemy Engine
    valid_df : DataFrame containing cleaned & validated records
    reject_df : DataFrame containing rejected records
    staging_table : SQLAlchemy table object
    reject_table : SQLAlchemy table object
    batch_size : int
        Number of rows per insert batch
    """

    if valid_df.empty and reject_df.empty:
        logger.info("No records to load.")
        return

    logger.info(
        f"Starting load | Valid rows: {len(valid_df)} | "
        f"Rejected rows: {len(reject_df)}"
    )

    try:
        with engine.begin() as connection:

            # -------------------------
            # Load Valid Records
            # -------------------------
            if not valid_df.empty:
                _batch_insert(
                    connection=connection,
                    table=staging_table,
                    df=valid_df,
                    batch_size=batch_size
                )
                logger.info(f"Inserted {len(valid_df)} valid rows into staging.")

            # -------------------------
            # Load Rejected Records
            # -------------------------
            if not reject_df.empty:
                prepared_rejects = _prepare_reject_records(reject_df)

                _batch_insert(
                    connection=connection,
                    table=reject_table,
                    df=prepared_rejects,
                    batch_size=batch_size
                )
                logger.info(f"Inserted {len(reject_df)} rejected rows.")

        logger.info("Load committed successfully.")

    except SQLAlchemyError:
        logger.exception("Database load failed. Transaction rolled back.")
        raise


# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------

def _batch_insert(
    connection,
    table,
    df: pd.DataFrame,
    batch_size: int
) -> None:
    """
    Perform batch inserts using SQLAlchemy Core.
    """

    records = df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]

        stmt = insert(table)

        # If your staging table has a primary key (booking_id),
        # you may optionally enable idempotency like this:
        #
        # stmt = stmt.on_conflict_do_nothing(
        #     index_elements=["booking_id"]
        # )
        #
        # Uncomment if required.

        connection.execute(stmt, batch)


def _prepare_reject_records(reject_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert rejected dataframe into schema expected by reject table.
    Assumes reject table structure:
        - source_name
        - raw_record (JSON or TEXT)
        - reject_reason
    """

    logger.info("Preparing rejected records.")

    reject_copy = reject_df.copy()

    reject_copy["raw_record"] = reject_copy.apply(
        lambda row: json.dumps(row.to_dict(), default=str),
        axis=1
    )

    prepared = pd.DataFrame({
        "source_name": "ride_bookings",
        "raw_record": reject_copy["raw_record"],
        "reject_reason": reject_copy["reject_reason"]
    })

    return prepared
