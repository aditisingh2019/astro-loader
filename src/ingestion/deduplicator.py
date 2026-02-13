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

def deduplicate(df : pd.DataFrame, viewed_records : set) -> pd.DataFrame:

    # Remove the duplicate records from current chunk, based on the unique booking id
    df = df[~df.duplicated(subset=['booking_id'], keep='first')]

    # Check previous records for duplicates 
    mask = ~df['booking_id'].isin(viewed_records)

    # Update the viewed records with current chunk
    viewed_records.update(df.loc[mask, 'booking_id'])

    return df.loc[mask]
