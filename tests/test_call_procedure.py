import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from sqlalchemy import text

from src.ingestion.call_procedure import call_procedure

@patch("src.ingestion.call_procedure.get_engine")
@patch("src.ingestion.call_procedure.execute_sql_file")
def test_call_procedure_with_default_engine(mock_execute_sql, mock_get_engine):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    call_procedure(engine=None)
    mock_get_engine.assert_called_once()
    mock_execute_sql.assert_called_once()
    sql_path = mock_execute_sql.call_args[0][1]
    assert sql_path.name == "transfer_stage_to_core.sql"

    mock_conn.execute.assert_called_once()
    called_stmt = mock_conn.execute.call_args[0][0]
    assert str(called_stmt) == str(text("CALL transfer_stage_to_core();"))


@patch("src.ingestion.call_procedure.execute_sql_file")
def test_call_procedure_with_passed_engine(mock_execute_sql):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    call_procedure(engine=mock_engine)

    mock_execute_sql.assert_called_once()
    mock_conn.execute.assert_called_once()
    called_stmt = mock_conn.execute.call_args[0][0]
    assert str(called_stmt) == str(text("CALL transfer_stage_to_core();"))


@patch("src.ingestion.call_procedure.get_engine")
@patch("src.ingestion.call_procedure.execute_sql_file")
def test_call_procedure_context_manager_called(mock_execute_sql, mock_get_engine):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    call_procedure()
    mock_engine.begin.assert_called_once()
    mock_engine.begin.return_value.__enter__.assert_called_once()
    mock_conn.execute.assert_called_once()
