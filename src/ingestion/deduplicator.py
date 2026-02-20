"""
Remove duplicate records from the dataset.
- Identify duplicates based on configured keys.
- Remove duplicate rows deterministically.
- Preserve the first or latest record based on rules.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def deduplicate(df : pd.DataFrame, viewed_records : set, subset : list=None) -> pd.DataFrame:

    if subset is None:
        subset = ['booking_id']

    # Remove the duplicate records from current chunk, based on the unique booking id.
    df = df[~df.duplicated(subset=subset, keep='first')]

    # Check if there are multiple elements to add to mask.
    if len(subset) == 1:
        keys = df[subset[0]]
    else:
        keys = df[subset].apply(tuple, axis=1)

     # Check previous records for duplicates .
    mask = ~keys.isin(viewed_records)

    # Update the viewed records with current chunk.
    viewed_records.update(keys[mask])

    return df.loc[mask]
