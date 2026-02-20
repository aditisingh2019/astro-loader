"""
Load processed data into PostgreSQL staging tables.
- Insert valid records into staging tables using PostgreSQL COPY (fast bulk insert).
- Insert rejected records into reject tables.
- Manage database transactions safely.
"""

from __future__ import annotations

import logging
import json
import pandas as pd
from io import StringIO
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Load valid and rejected records into database
def load_data(
    engine: Engine,
    valid_df: pd.DataFrame,
    reject_df: pd.DataFrame,
    staging_table,
    reject_table,
    batch_size: int = 10000
) -> None:

    if valid_df.empty and reject_df.empty:
        return

    try:
        with engine.begin() as connection:

            # Load Valid Records
            if not valid_df.empty:
                _batch_insert(
                    connection=connection,
                    table=staging_table,
                    df=valid_df,
                    batch_size=batch_size
                )

    # Load Rejected Records (use standard inserts for JSON data)
            if not reject_df.empty:
                prepared_rejects = _prepare_reject_records(reject_df)
                _batch_insert_fallback(
                    connection=connection,
                    table=reject_table,
                    df=prepared_rejects,
                    batch_size=batch_size
                )

    except SQLAlchemyError:
        logger.exception("Database load failed. Transaction rolled back.")
        raise


# Perform batch inserts using PostgreSQL COPY
def _batch_insert(
    connection,
    table,
    df: pd.DataFrame,
    batch_size: int = 10000
) -> None:

    table_col_names = [col.name for col in table.columns]
    
    df_filtered = df[[col for col in df.columns if col in table_col_names]].copy()
    
    if df_filtered.empty:
        logger.warning(f"No matching columns found for {table.name}")
        return
    
    table_name = table.name
    
    try:
        raw_conn = connection.connection
        cursor = raw_conn.cursor()
        
        buffer = StringIO()
        df_filtered.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
        buffer.seek(0)

        col_names = list(df_filtered.columns)
        cursor.copy_from(
            buffer,
            table_name,
            columns=col_names,
            null='\\N'
        )
        
        cursor.close()
        
    except Exception as e:
        logger.warning(f"COPY operation failed for {table_name}. Falling back to INSERT.")
        _batch_insert_fallback(connection, table, df, batch_size)



def _batch_insert_fallback(
    connection,
    table,
    df: pd.DataFrame,
    batch_size: int
) -> None:
    
    records = df.to_dict(orient="records")
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        stmt = insert(table)
        connection.execute(stmt, batch)

# Convert rejected dataframe into schema expected by reject table.
def _prepare_reject_records(reject_df: pd.DataFrame) -> pd.DataFrame:

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
