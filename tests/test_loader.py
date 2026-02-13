import json
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.ingestion.loader import load_data, _prepare_reject_records

# Fixtures
@pytest.fixture
def mock_engine():
    engine = MagicMock()
    connection = MagicMock()

    engine.begin.return_value.__enter__.return_value = connection
    engine.begin.return_value.__exit__.return_value = False

    return engine, connection


@pytest.fixture
def sample_valid_df():
    return pd.DataFrame([
        {"ride_id": 1, "city": "NY"},
        {"ride_id": 2, "city": "LA"},
    ])


@pytest.fixture
def sample_reject_df():
    return pd.DataFrame([
        {"ride_id": 3, "reject_reason": "missing city"},
        {"ride_id": 4, "reject_reason": "invalid fare"},
    ])


@pytest.fixture
def mock_tables():
    staging = MagicMock(name="staging_table")
    reject = MagicMock(name="reject_table")
    return staging, reject

# Tests
def test_load_data_no_rows(mock_engine, mock_tables):
    engine, connection = mock_engine
    staging, reject = mock_tables

    load_data(engine, pd.DataFrame(), pd.DataFrame(), staging, reject)

    engine.begin.assert_not_called()


def test_load_valid_records_only(
    mock_engine, sample_valid_df, mock_tables
):
    engine, connection = mock_engine
    staging, reject = mock_tables

    load_data(engine, sample_valid_df, pd.DataFrame(), staging, reject)

    engine.begin.assert_called_once()
    assert connection.execute.call_count >= 1


def test_load_rejected_records_only(
    mock_engine, sample_reject_df, mock_tables
):
    engine, connection = mock_engine
    staging, reject = mock_tables

    load_data(engine, pd.DataFrame(), sample_reject_df, staging, reject)

    engine.begin.assert_called_once()
    assert connection.execute.call_count >= 1


def test_prepare_reject_records():
    reject_df = pd.DataFrame([
        {"ride_id": 1, "reject_reason": "bad data"}
    ])

    prepared = _prepare_reject_records(reject_df)

    assert "source_name" in prepared.columns
    assert "raw_record" in prepared.columns
    assert "reject_reason" in prepared.columns

    assert prepared.iloc[0]["source_name"] == "ride_bookings"
    assert isinstance(prepared.iloc[0]["raw_record"], str)

    parsed = json.loads(prepared.iloc[0]["raw_record"])
    assert parsed["ride_id"] == 1


def test_load_rollback_on_error(
    mock_engine, sample_valid_df, mock_tables
):
    engine, connection = mock_engine
    staging, reject = mock_tables

    connection.execute.side_effect = Exception("DB failure")

    with pytest.raises(Exception):
        load_data(engine, sample_valid_df, pd.DataFrame(), staging, reject)

    engine.begin.assert_called_once()
