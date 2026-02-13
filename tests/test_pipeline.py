import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.ingestion.pipeline import run_pipeline


# Fixtures
@pytest.fixture
def sample_chunk():
    return pd.DataFrame([
        {"ride_id": 1},
        {"ride_id": 2},
    ])


@pytest.fixture
def reader_fixture(sample_chunk):
    return [sample_chunk]


# Tests
@patch("src.ingestion.pipeline.load_data")
@patch("src.ingestion.pipeline.deduplicate")
@patch("src.ingestion.pipeline.clean_dataframe")
@patch("src.ingestion.pipeline.validate_dataframe")
@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
@patch("src.ingestion.pipeline.setup_logger")
def test_pipeline_happy_path(
    mock_logger,
    mock_engine,
    mock_read,
    mock_validate,
    mock_clean,
    mock_dedupe,
    mock_load,
    reader_fixture,
    sample_chunk
):

    logger = MagicMock()
    mock_logger.return_value = logger
    mock_engine.return_value = MagicMock()

    mock_read.return_value = reader_fixture

    valid_df = sample_chunk.copy()
    reject_df = pd.DataFrame([])

    mock_validate.return_value = (valid_df, reject_df)
    mock_clean.return_value = valid_df
    mock_dedupe.return_value = valid_df

    run_pipeline("test.csv", chunksize=100)

    mock_read.assert_called_once()
    mock_validate.assert_called()
    mock_clean.assert_called()
    mock_dedupe.assert_called()
    mock_load.assert_called()

    logger.info.assert_called()


@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.setup_logger")
@patch("src.ingestion.pipeline.get_engine")
def test_pipeline_handles_empty_reader(
    mock_engine,
    mock_logger,
    mock_read
):
    logger = MagicMock()
    mock_logger.return_value = logger
    mock_engine.return_value = MagicMock()

    mock_read.return_value = []

    run_pipeline("empty.csv")

    logger.info.assert_called()


@patch("src.ingestion.pipeline.load_data")
@patch("src.ingestion.pipeline.deduplicate")
@patch("src.ingestion.pipeline.clean_dataframe")
@patch("src.ingestion.pipeline.validate_dataframe")
@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
@patch("src.ingestion.pipeline.setup_logger")
def test_pipeline_error_propagates(
    mock_logger,
    mock_engine,
    mock_read,
    mock_validate,
    mock_clean,
    mock_dedupe,
    mock_load
):

    logger = MagicMock()
    mock_logger.return_value = logger
    mock_engine.return_value = MagicMock()

    chunk = pd.DataFrame([{"ride_id": 1}])
    mock_read.return_value = [chunk]

    mock_validate.side_effect = Exception("validation failure")

    with pytest.raises(Exception):
        run_pipeline("bad.csv")

    logger.exception.assert_called()
