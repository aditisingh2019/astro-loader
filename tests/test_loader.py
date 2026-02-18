import pytest
import pandas as pd
import json
from unittest.mock import MagicMock, patch

from src.ingestion.loader import (
    load_data,
    _prepare_reject_records,
    _batch_insert,
)
class DummyTable:
    name = "dummy_table"


class DummyConnection:
    def __init__(self):
        self.executed = []

    def execute(self, stmt, batch):
        self.executed.append((stmt, batch))


class DummyEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return DummyContextManager(self.connection)


class DummyContextManager:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False

def test_prepare_reject_records_structure():
    df = pd.DataFrame({
        "booking_id": [1],
        "reject_reason": ["Invalid value"]
    })

    prepared = _prepare_reject_records(df)

    assert list(prepared.columns) == [
        "source_name",
        "raw_record",
        "reject_reason",
    ]

    assert prepared.loc[0, "source_name"] == "ride_bookings"

    raw = json.loads(prepared.loc[0, "raw_record"])
    assert raw["booking_id"] == 1
    assert prepared.loc[0, "reject_reason"] == "Invalid value"

def test_batch_insert_splits_batches_correctly():
    df = pd.DataFrame({
        "col": list(range(10))
    })

    connection = DummyConnection()
    table = MagicMock()

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_insert.return_value = MagicMock()

        _batch_insert(connection, table, df, batch_size=3)

        assert len(connection.executed) == 4

        total_inserted = sum(len(batch) for _, batch in connection.executed)
        assert total_inserted == 10

def test_load_data_returns_early_if_both_empty():
    engine = MagicMock()
    load_data(
        engine,
        pd.DataFrame(),
        pd.DataFrame(),
        staging_table=DummyTable(),
        reject_table=DummyTable(),
    )

    engine.begin.assert_not_called()


def test_load_data_inserts_only_valid_records():
    valid_df = pd.DataFrame({"a": [1, 2]})
    reject_df = pd.DataFrame()

    connection = DummyConnection()
    engine = DummyEngine(connection)

    with patch("src.ingestion.loader._batch_insert") as mock_batch:
        load_data(
            engine,
            valid_df,
            reject_df,
            staging_table=DummyTable(),
            reject_table=DummyTable(),
            batch_size=2
        )

        mock_batch.assert_called_once()
        args = mock_batch.call_args[0]
        assert args[2].equals(valid_df)


def test_load_data_inserts_only_reject_records():
    valid_df = pd.DataFrame()
    reject_df = pd.DataFrame({
        "col": [1],
        "reject_reason": ["Bad"]
    })

    connection = DummyConnection()
    engine = DummyEngine(connection)

    with patch("src.ingestion.loader._batch_insert") as mock_batch:
        load_data(
            engine,
            valid_df,
            reject_df,
            staging_table=DummyTable(),
            reject_table=DummyTable(),
        )

        assert mock_batch.call_count == 1

        inserted_df = mock_batch.call_args[0][2]
        assert "raw_record" in inserted_df.columns
        assert inserted_df.loc[0, "source_name"] == "ride_bookings"


def test_load_data_inserts_both_valid_and_reject():
    valid_df = pd.DataFrame({"a": [1]})
    reject_df = pd.DataFrame({
        "b": [2],
        "reject_reason": ["Bad"]
    })

    connection = DummyConnection()
    engine = DummyEngine(connection)

    with patch("src.ingestion.loader._batch_insert") as mock_batch:
        load_data(
            engine,
            valid_df,
            reject_df,
            staging_table=DummyTable(),
            reject_table=DummyTable(),
        )
        assert mock_batch.call_count == 2


def test_load_data_raises_on_exception_and_propagates():
    valid_df = pd.DataFrame({"a": [1]})
    reject_df = pd.DataFrame()

    class FailingConnection(DummyConnection):
        def execute(self, stmt, batch):
            raise RuntimeError("DB failure")

    engine = DummyEngine(FailingConnection())

    with patch("src.ingestion.loader.insert") as mock_insert:
        mock_insert.return_value = MagicMock()

        with pytest.raises(RuntimeError):
            load_data(
                engine,
                valid_df,
                reject_df,
                staging_table=MagicMock(),
                reject_table=MagicMock(),
            )
