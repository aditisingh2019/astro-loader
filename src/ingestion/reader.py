"""
Read raw data from external file sources into memory.
- Support reading CSV and JSON files.
- Return data as a Pandas DataFrame.
- Handle file-level errors gracefully.
"""

import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

PATH_EXTENSIONS = [".csv", ".json"]

def read_file(filename : str, chunksize: int) -> pd.DataFrame:

    # Check if file exists
    if not os.path.exists(filename):
        raise FileNotFoundError("File not found")
    
    # Obtain the file extension
    extension = os.path.splitext(filename)[1].lower()

    # Check if file extension is defined in opening functions
    if extension not in PATH_EXTENSIONS:
        raise ValueError(f"File extension {extension} not supported")
    
    try:
        if extension == '.csv':
            return pd.read_csv(filename, chunksize=chunksize)
        if extension == '.json':
            return pd.read_json(filename, lines=True, chunksize=chunksize)

    except Exception as e:
        logger.error("Failed to load data into dataframe")
        raise
