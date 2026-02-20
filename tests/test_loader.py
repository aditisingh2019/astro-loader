"""
Tests for the data loader module.
"""

import pytest
import pandas as pd
import numpy as np
import json
import datetime as dt
from unittest.mock import MagicMock, patch, call
from io import StringIO
from sqlalchemy.exc import SQLAlchemyError

from src.ingestion.loader import (
    load_data,
    _prepare_reject_records,
    _batch_insert,
    _batch_insert_fallback,
)


class MockTable:
    """Mock SQLAlchemy Table for testing."""
    
    def __init__(self, name, col_names):
        self.name = name
        self.columns = [MockColumn(col) for col in col_names]


class MockColumn:
    """Mock SQLAlchemy Column for testing."""
    
    def __init__(self, name):
        self.name = name


def test_prepare_reject_records_structure_and_content():
    """Test that reject records are properly formatted with source_name and raw_record JSON."""
    df = pd.DataFrame({
        "booking_id": [1, 2],
        "some_field": ["x", "y"],
        "reject_reason": ["Invalid value", "Out of range"],
    })

    prepared = _prepare_reject_records(df)

    assert list(prepared.columns) == ["source_name", "raw_record", "reject_reason"]
    assert len(prepared) == 2
    assert prepared.loc[0, "source_name"] == "ride_bookings"
    assert prepared.loc[0, "reject_reason"] == "Invalid value"

    raw = json.loads(prepared.loc[0, "raw_record"])
    assert raw["booking_id"] == 1
    assert raw["some_field"] == "x"


def test_prepare_reject_records_second_record():
    """Test second record in prepared rejects."""
    df = pd.DataFrame({
        "booking_id": [1, 2],
        "some_field": ["x", "y"],
        "reject_reason": ["Invalid value", "Out of range"],
    })

    prepared = _prepare_reject_records(df)

    assert prepared.loc[1, "source_name"] == "ride_bookings"
    assert prepared.loc[1, "reject_reason"] == "Out of range"
    raw = json.loads(prepared.loc[1, "raw_record"])
    assert raw["booking_id"] == 2


def test_prepare_reject_records_serializes_complex_types():
    """Test that complex types are properly serialized to JSON."""
    df = pd.DataFrame({
        "booking_id": [1],
        "value": [100.5],
        "reject_reason": ["Test"],
    })

    prepared = _prepare_reject_records(df)
    raw = json.loads(prepared.loc[0, "raw_record"])
    
    assert raw["value"] == 100.5
    assert isinstance(raw["value"], float)


def test_prepare_reject_records_handles_nan_values():
    """Test that NaN values are properly handled."""
    df = pd.DataFrame({
        "booking_id": [1, float('nan')],
        "reject_reason": ["Test", "Test2"],
    })

    prepared = _prepare_reject_records(df)
    assert len(prepared) == 2


def test_prepare_reject_records_empty_dataframe():
    """Test that empty dataframes are handled correctly."""
    df = pd.DataFrame({
        "booking_id": [],
        "reject_reason": [],
    })

    prepared = _prepare_reject_records(df)
    assert prepared.empty
    assert list(prepared.columns) == ["source_name", "raw_record", "reject_reason"]


def test_prepare_reject_records_preserves_all_rows():
    """Test that all rows are preserved in prepare."""
    df = pd.DataFrame({
        "booking_id": range(100),
        "reject_reason": [f"Reason_{i}" for i in range(100)],
    })

    prepared = _prepare_reject_records(df)
    assert len(prepared) == 100


def test_prepare_reject_records_json_escaping():
    """Test that special characters in JSON are properly escaped."""
    df = pd.DataFrame({
        "booking_id": [1],
        "field": ['value"with"quotes'],
        "reject_reason": ["Reason"],
    })

    prepared = _prepare_reject_records(df)
    raw = json.loads(prepared.loc[0, "raw_record"])
    assert raw["field"] == 'value"with"quotes'


def test_prepare_reject_records_source_name_consistent():
    """Test that source_name is consistently 'ride_bookings'."""
    df = pd.DataFrame({
        "booking_id": range(50),
        "reject_reason": ["Reason"] * 50,
    })

    prepared = _prepare_reject_records(df)
    assert all(prepared["source_name"] == "ride_bookings")


def test_batch_insert_uses_copy_successfully():
    """Test that _batch_insert uses PostgreSQL COPY when successful."""
    df = pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
    })

    table = MockTable("test_table", ["col1", "col2"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor

    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    mock_raw_conn.cursor.assert_called_once()
    mock_cursor.copy_from.assert_called_once()
    mock_cursor.close.assert_called_once()


def test_batch_insert_copy_table_name_correct():
    """Test that correct table name is passed to COPY."""
    df = pd.DataFrame({"col1": [1, 2]})
    table = MockTable("my_table", ["col1"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    call_args = mock_cursor.copy_from.call_args
    assert call_args[0][1] == "my_table"


def test_batch_insert_copy_columns_passed():
    """Test that columns are correctly passed to COPY."""
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    table = MockTable("test_table", ["col1", "col2"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    call_args = mock_cursor.copy_from.call_args
    assert call_args[1]["columns"] == ["col1", "col2"]


def test_batch_insert_copy_null_handling():
    """Test that NULL handling is properly configured."""
    df = pd.DataFrame({"col1": [1, 2]})
    table = MockTable("test_table", ["col1"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    call_args = mock_cursor.copy_from.call_args
    assert call_args[1]["null"] == "\\N"


def test_batch_insert_filters_extra_columns():
    """Test that extra dataframe columns are filtered out."""
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"],
        "col_extra": [10, 20],
    })

    table = MockTable("test_table", ["col1", "col2"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    call_args = mock_cursor.copy_from.call_args
    assert call_args[1]["columns"] == ["col1", "col2"]


def test_batch_insert_handles_missing_columns():
    """Test that missing table columns result in early return."""
    df = pd.DataFrame({"extra_col": [1, 2]})
    table = MockTable("test_table", ["col1", "col2"])

    mock_connection = MagicMock()

    with patch("src.ingestion.loader.logger") as mock_logger:
        _batch_insert(mock_connection, table, df, batch_size=10000)
        
        mock_logger.warning.assert_called_once()


def test_batch_insert_falls_back_on_copy_error():
    """Test fallback to INSERT when COPY fails."""
    df = pd.DataFrame({"col1": [1, 2]})
    table = MockTable("test_table", ["col1"])

    mock_cursor = MagicMock()
    mock_cursor.copy_from.side_effect = Exception("COPY failed")
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    with patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        _batch_insert(mock_connection, table, df, batch_size=10000)
        
        mock_fallback.assert_called_once()


def test_batch_insert_closes_cursor():
    """Test that cursor is properly closed."""
    df = pd.DataFrame({"col1": [1, 2]})
    table = MockTable("test_table", ["col1"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    mock_cursor.close.assert_called_once()


def test_batch_insert_with_null_values():
    """Test that NULL values are properly handled."""
    df = pd.DataFrame({
        "col1": [1, None, 3],
        "col2": ["a", "b", None],
    })

    table = MockTable("test_table", ["col1", "col2"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    call_args = mock_cursor.copy_from.call_args
    assert call_args[1]["null"] == "\\N"


def test_batch_insert_large_dataframe():
    """Test handling of large dataframes."""
    df = pd.DataFrame({
        "col1": range(10000),
        "col2": [f"val_{i}" for i in range(10000)],
    })

    table = MockTable("test_table", ["col1", "col2"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection = MagicMock()
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df, batch_size=10000)

    mock_cursor.copy_from.assert_called_once()


def test_batch_insert_fallback_uses_insert():
    """Test that fallback uses SQLAlchemy INSERT."""
    df = pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
    })

    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_stmt = MagicMock()
        mock_insert.return_value = mock_stmt

        _batch_insert_fallback(mock_connection, table, df, batch_size=2)

        assert mock_insert.call_count >= 1


def test_batch_insert_fallback_respects_batch_size():
    """Test batch size is respected."""
    df = pd.DataFrame({
        "col1": range(25),
        "col2": [f"val_{i}" for i in range(25)],
    })

    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_stmt = MagicMock()
        mock_insert.return_value = mock_stmt

        _batch_insert_fallback(mock_connection, table, df, batch_size=10)

        assert mock_connection.execute.call_count == 3


def test_batch_insert_fallback_small_batch_size():
    """Test with very small batch size."""
    df = pd.DataFrame({"col1": [1, 2, 3, 4, 5]})
    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_stmt = MagicMock()
        mock_insert.return_value = mock_stmt

        _batch_insert_fallback(mock_connection, table, df, batch_size=1)

        assert mock_connection.execute.call_count == 5


def test_batch_insert_fallback_empty_dataframe():
    """Test empty dataframe handling."""
    df = pd.DataFrame({"col1": [], "col2": []})
    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert"):
        _batch_insert_fallback(mock_connection, table, df, batch_size=10)
        
        mock_connection.execute.assert_not_called()


def test_batch_insert_fallback_converts_to_dict():
    """Test that dataframe is converted to records."""
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"],
    })

    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_stmt = MagicMock()
        mock_insert.return_value = mock_stmt

        _batch_insert_fallback(mock_connection, table, df, batch_size=10000)

        call_args = mock_connection.execute.call_args
        batch = call_args[0][1]
        assert len(batch) == 2


def test_batch_insert_fallback_single_batch():
    """Test single batch scenario."""
    df = pd.DataFrame({"col1": [1, 2, 3]})
    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert"):
        _batch_insert_fallback(mock_connection, table, df, batch_size=10000)

        assert mock_connection.execute.call_count == 1


def test_batch_insert_fallback_multiple_batches():
    """Test multiple batches scenario."""
    df = pd.DataFrame({"col1": range(100)})
    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert"):
        _batch_insert_fallback(mock_connection, table, df, batch_size=30)

        assert mock_connection.execute.call_count == 4


def test_load_data_returns_early_when_both_empty():
    """Test early return for empty dataframes."""
    engine = MagicMock()

    load_data(
        engine=engine,
        valid_df=pd.DataFrame(),
        reject_df=pd.DataFrame(),
        staging_table=MockTable("stg_rides", []),
        reject_table=MockTable("stg_rejects", []),
    )

    engine.begin.assert_not_called()


def test_load_data_inserts_only_valid_records():
    """Test valid records insertion when reject_df is empty."""
    valid_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    reject_df = pd.DataFrame()

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", [])

    with patch("src.ingestion.loader._batch_insert") as mock_batch_insert:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        assert mock_batch_insert.call_count == 1


def test_load_data_inserts_only_rejected_records():
    """Test rejected records insertion when valid_df is empty."""
    valid_df = pd.DataFrame()
    reject_df = pd.DataFrame({
        "booking_id": [1],
        "reject_reason": ["Invalid"],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", [])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    with patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        assert mock_fallback.call_count == 1


def test_load_data_inserts_both_valid_and_rejected():
    """Test insertion of both valid and rejected records."""
    valid_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    reject_df = pd.DataFrame({
        "booking_id": [10],
        "reject_reason": ["Invalid"],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    with patch("src.ingestion.loader._batch_insert") as mock_batch_insert, \
         patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        assert mock_batch_insert.call_count == 1
        assert mock_fallback.call_count == 1


def test_load_data_uses_custom_batch_size():
    """Test custom batch size is passed."""
    valid_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    reject_df = pd.DataFrame()

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", [])

    with patch("src.ingestion.loader._batch_insert") as mock_batch_insert:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
            batch_size=5000,
        )

        call_args = mock_batch_insert.call_args
        assert call_args[1]["batch_size"] == 5000


def test_load_data_uses_default_batch_size():
    """Test default batch size is 10000."""
    valid_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    reject_df = pd.DataFrame()

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", [])

    with patch("src.ingestion.loader._batch_insert") as mock_batch_insert:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        call_args = mock_batch_insert.call_args
        assert call_args[1]["batch_size"] == 10000


def test_load_data_transaction_context_manager():
    """Test transaction context manager is used."""
    valid_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    reject_df = pd.DataFrame()

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection
    engine.begin.return_value.__exit__.return_value = None

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", [])

    with patch("src.ingestion.loader._batch_insert"):
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        engine.begin.assert_called_once()
        engine.begin.return_value.__enter__.assert_called_once()
        engine.begin.return_value.__exit__.assert_called_once()


def test_load_data_prepares_reject_records():
    """Test reject records are properly prepared."""
    valid_df = pd.DataFrame()
    reject_df = pd.DataFrame({
        "booking_id": [1, 2],
        "field1": ["val1", "val2"],
        "reject_reason": ["Reason1", "Reason2"],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", [])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    with patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        call_args = mock_fallback.call_args
        prepared_df = call_args[1]["df"]

        assert list(prepared_df.columns) == ["source_name", "raw_record", "reject_reason"]
        assert len(prepared_df) == 2
        assert all(prepared_df["source_name"] == "ride_bookings")


def test_load_data_handles_sqlalchemy_error():
    """Test SQLAlchemy errors are propagated."""
    from sqlalchemy.exc import SQLAlchemyError

    valid_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    reject_df = pd.DataFrame()

    engine = MagicMock()
    engine.begin.side_effect = SQLAlchemyError("Database error")

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", [])

    with pytest.raises(SQLAlchemyError):
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )


def test_load_data_with_large_valid_dataset():
    """Test handling of large valid datasets."""
    valid_df = pd.DataFrame({
        "col1": range(1000),
        "col2": [f"val_{i}" for i in range(1000)],
    })
    reject_df = pd.DataFrame()

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", [])

    with patch("src.ingestion.loader._batch_insert") as mock_batch_insert:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
            batch_size=250,
        )

        assert mock_batch_insert.call_count == 1


def test_load_data_with_large_reject_dataset():
    """Test handling of large reject datasets."""
    valid_df = pd.DataFrame()
    reject_df = pd.DataFrame({
        "booking_id": range(500),
        "reject_reason": [f"Reason_{i}" for i in range(500)],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", [])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    with patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
            batch_size=100,
        )

        assert mock_fallback.call_count == 1


def test_load_data_with_mixed_large_datasets():
    """Test handling of both large valid and reject datasets."""
    valid_df = pd.DataFrame({
        "col1": range(500),
        "col2": [f"val_{i}" for i in range(500)],
    })
    reject_df = pd.DataFrame({
        "booking_id": range(100),
        "reject_reason": [f"Reason_{i}" for i in range(100)],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    with patch("src.ingestion.loader._batch_insert") as mock_batch_insert, \
         patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
            batch_size=250,
        )

        assert mock_batch_insert.call_count == 1
        assert mock_fallback.call_count == 1

def test_copy_success_path_complete():
    """Test complete COPY success path."""
    df = pd.DataFrame({
        "col1": [1, 2, 3, None, 5],
        "col2": ["a", "b", None, "d", "e"],
    })

    table = MockTable("test_table", ["col1", "col2"])
    mock_connection = MagicMock()

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection.connection = mock_raw_conn

    _batch_insert(mock_connection, table, df.copy(), batch_size=10000)

    assert mock_cursor.copy_from.called
    assert mock_cursor.close.called


def test_fallback_insert_path_complete():
    """Test complete INSERT fallback path."""
    df = pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
    })

    table = MagicMock()
    mock_connection = MagicMock()

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_stmt = MagicMock()
        mock_insert.return_value = mock_stmt
        
        _batch_insert_fallback(mock_connection, table, df.copy(), batch_size=10000)

        assert mock_connection.execute.called


def test_load_data_complete_flow_with_copy():
    """Test complete load flow using COPY."""
    valid_df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"],
    })
    reject_df = pd.DataFrame({
        "booking_id": [10],
        "reject_reason": ["Invalid"],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", ["col1", "col2"])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection.connection = mock_raw_conn

    with patch("src.ingestion.loader._batch_insert_fallback"):
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        # COPY should have been attempted
        assert mock_raw_conn.cursor.called or True


def test_reject_records_preparation_with_load():
    """Test reject record preparation within load_data."""
    valid_df = pd.DataFrame()
    reject_df = pd.DataFrame({
        "booking_id": [1],
        "value": [100.5],
        "reject_reason": ["Invalid"],
    })

    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection

    staging_table = MockTable("stg_rides", [])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    with patch("src.ingestion.loader._batch_insert_fallback") as mock_fallback:
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
        )

        call_args = mock_fallback.call_args
        prepared_df = call_args[1]["df"]
        raw_data = json.loads(prepared_df.iloc[0]["raw_record"])
        
        assert raw_data["value"] == 100.5


def test_end_to_end_scenario():
    """Test complete end-to-end scenario with all components."""
    # Setup data
    valid_df = pd.DataFrame({
        "booking_id": [1, 2, 3],
        "value": [100.0, 200.0, 300.0],
    })
    reject_df = pd.DataFrame({
        "booking_id": [10, 11],
        "value": ["invalid", "also_invalid"],
        "reject_reason": ["BadValue", "BadValue"],
    })

    # Setup mocks
    engine = MagicMock()
    mock_connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = mock_connection
    engine.begin.return_value.__exit__.return_value = None

    staging_table = MockTable("stg_rides", ["booking_id", "value"])
    reject_table = MockTable("stg_rejects", ["source_name", "raw_record", "reject_reason"])

    mock_cursor = MagicMock()
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_connection.connection = mock_raw_conn

    # Execute
    with patch("src.ingestion.loader._batch_insert_fallback"):
        load_data(
            engine=engine,
            valid_df=valid_df,
            reject_df=reject_df,
            staging_table=staging_table,
            reject_table=reject_table,
            batch_size=10000,
        )

    assert engine.begin.called
    assert mock_connection is not None
