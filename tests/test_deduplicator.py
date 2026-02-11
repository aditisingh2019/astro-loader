import pytest
import logging
import pandas as pd
from src.ingestion import deduplicator
from pandas.testing import assert_frame_equal

mock_data_with_duplicates = pd.DataFrame({
    'id' : [1, 2, 3, 4, 2, 5, 3],
    'name' : ['Gamma', 'Beta', 'Omega', 'Lambda', 'Beta', 'Epsilon', 'Omega'],
})

mock_data_without_duplicates = pd.DataFrame({
    'id' : [1, 2, 3, 4, 5,],
    'name' : ['Gamma', 'Beta', 'Omega', 'Lambda', 'Epsilon'],
})

def test_deduplicator_success():

    df = pd.DataFrame(mock_data_with_duplicates)
    df.drop_duplicates(inplace=True)

    result_df = deduplicator.deduplicate(mock_data_with_duplicates)

    assert result_df.shape == df.shape

def test_deduplicator_log_with_record_success(caplog):

    df = pd.DataFrame(mock_data_with_duplicates)

    caplog.set_level(logging.INFO)
    deduplicator.deduplicate(df)
    assert "There were 2 duplicate records." in caplog.text

def test_deduplicator_log_no_record_success(caplog):

    df = pd.DataFrame(mock_data_without_duplicates)

    caplog.set_level(logging.INFO)
    deduplicator.deduplicate(df)
    
    assert caplog.text == ""
    