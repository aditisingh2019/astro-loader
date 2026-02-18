import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from src.db.connection import (
    get_engine,
    execute_sql_file,
    setup_database,
)

@patch("src.db.connection.create_engine")
@patch("src.db.connection.load_dotenv")
def test_get_engine_success(mock_load_dotenv, mock_create_engine):
    env = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "testdb",
        "DB_USER": "user",
        "DB_PASSWORD": "pass",
    }

    with patch.dict(os.environ, env, clear=True):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        engine = get_engine()

        assert engine == mock_engine
        mock_load_dotenv.assert_called_once()
        mock_create_engine.assert_called_once()

        args, kwargs = mock_create_engine.call_args
        assert kwargs["pool_size"] == 5
        assert kwargs["max_overflow"] == 10
        assert kwargs["pool_pre_ping"] is True
        assert kwargs["future"] is True

@patch("src.db.connection.load_dotenv")
def test_get_engine_missing_env_vars(mock_load_dotenv):
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(OSError):
            get_engine()


@patch("src.db.connection.create_engine")
@patch("src.db.connection.load_dotenv")
def test_get_engine_engine_creation_failure(mock_load_dotenv, mock_create_engine):
    env = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "testdb",
        "DB_USER": "user",
        "DB_PASSWORD": "pass",
    }

    with patch.dict(os.environ, env, clear=True):
        mock_create_engine.side_effect = RuntimeError("Engine failure")

        with pytest.raises(RuntimeError):
            get_engine()


def test_execute_sql_file_success(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT 1;")

    mock_engine = MagicMock()
    mock_conn = MagicMock()

    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    execute_sql_file(mock_engine, sql_file)

    mock_engine.begin.assert_called_once()
    mock_conn.execute.assert_called_once()


def test_execute_sql_file_missing_file(tmp_path):
    mock_engine = MagicMock()
    missing_file = tmp_path / "does_not_exist.sql"

    with pytest.raises(FileNotFoundError):
        execute_sql_file(mock_engine, missing_file)

@patch("src.db.connection.execute_sql_file")
@patch("src.db.connection.get_engine")
def test_setup_database_calls_files_in_order(
    mock_get_engine,
    mock_execute_sql,
):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    setup_database()

    assert mock_execute_sql.call_count == 3

    calls = mock_execute_sql.call_args_list

    assert "staging.sql" in str(calls[0])
    assert "core.sql" in str(calls[1])
    assert "transfer_stage_to_core.sql" in str(calls[2])
    
    for c in calls:
        assert c.args[0] == mock_engine
