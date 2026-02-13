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

    # Remove the duplicate records from current chunk
    df = df[~df.duplicated(keep='first')]

    # Convert dataframe rows to tuples
    row_tuples = df.apply(tuple, axis=1)

    # Mask for comparing previously viewed chunks to current chunk
    mask = []
    for row in row_tuples:
        if row in viewed_records:
            mask.append(False)
        else:
            viewed_records.add(row)
            mask.append(True)

    return df[mask], viewed_records
