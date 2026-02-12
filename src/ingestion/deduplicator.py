"""
Purpose:
--------
Remove duplicate records from the dataset.

Responsibilities:
-----------------
- Identify duplicates based on configured keys.
- Remove duplicate rows deterministically.
- Preserve the first or latest record based on rules.

Important Behavior:
-------------------
- Deduplication logic is config-driven.
- Runs after cleaning and before loading.
- Does not affect rejected records.

Design Notes:
-------------
- Supports single or composite keys.
- Designed for performance on medium-sized datasets.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def deduplicate(df : pd.DataFrame) -> pd.DataFrame:

    # Create copy of duplcates for saving to log
    duplicates_df = df[df.duplicated()]

    # Make log record of number of duplicates in file
    if duplicates_df.shape[0] > 0:
        logger.info(f"There were {duplicates_df.shape[0]} duplicate records.")

    # Drop the dupliated rows from the dataframe
    deduplicated_df = df.drop_duplicates()
    return deduplicated_df
