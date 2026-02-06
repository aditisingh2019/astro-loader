"""
Purpose:
--------
Read raw data from external file sources into memory.

Responsibilities:
-----------------
- Support reading CSV and JSON files.
- Return data as a Pandas DataFrame.
- Handle file-level errors gracefully.

Important Behavior:
-------------------
- File type selection is config-driven.
- Does not perform validation or cleaning.
- Raises clear errors for unreadable files.

Design Notes:
-------------
- Designed for easy extension to new formats (e.g., Parquet).
- Keeps I/O logic isolated from business logic.
"""

import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

PATH_EXTENSIONS = [".csv", ".json"]

def read_file(filename : str) -> pd.DataFrame:

    # Check if file exists
    if not os.path.exists(filename):
        raise FileNotFoundError("File not found")
    
    # Obtain the file extension
    extension = os.path.splitext(filename)[1]

    # Check if file extension is defined in opening functions
    if extension not in PATH_EXTENSIONS:
        raise ValueError(f"File extension {extension} not supported")
    
    try:
        if extension == '.csv':
            df = pd.read_csv(filename)
            return df
        elif extension == '.json':
            df = pd.read_json(filename)
            return df
        else:
            logger.error("An unknown error has occurred")

    except Exception:
        logger.error("Failed to load data into dataframe")
