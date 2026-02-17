import pytest
import pandas as pd
from src.ingestion.call_procedure import call_procedure
from unittest.mock import MagicMock

@pytest.fixture
def mock_procedure_name():
    return "test"

@pytest.fixture
def mock_procedure_parameters():
    return {"username": "user1", "password" : 1234}

@pytest.fixture
def mock_connection():

    mock_engine = MagicMock()
    mock_connection = MagicMock()

    mock_connection.__enter__.return_value= mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine.begin.return_value.__enter__.return_value = mock_connection
    mock_engine.begin.return_value.__exit__.return_value = None

    return mock_engine, mock_connection

# Test procedure call with only engine passed to function.
def test_call_procedure_no_parameters(mock_connection):

    engine, connection = mock_connection
    call_procedure(engine=engine)

    connection.execute.assert_called_once()
    executed_call = str(connection.execute.call_args[0][0])

    assert "CALL transfer_uber_data();" in executed_call

# Test procedure call with engine and named procedure.
def test_call_procedure_with_procedure_name_no_parameters(mock_procedure_name, mock_connection):

    engine, connection = mock_connection
    call_procedure(engine=engine, procedure_name=mock_procedure_name, procedure_params=None)

    connection.execute.assert_called_once()
    executed_call = str(connection.execute.call_args[0][0])

    assert f"CALL {mock_procedure_name}();" in executed_call


# Test procedure call with engine, named procedure, and list of procedure parameters.
def test_call_procedure_with_procedure_name_and_parameters(mock_procedure_name, mock_procedure_parameters, mock_connection):

    engine, connection = mock_connection
    call_procedure(engine=engine, procedure_name=mock_procedure_name, procedure_params=mock_procedure_parameters)

    connection.execute.assert_called_once()
    executed_call = str(connection.execute.call_args[0][0])

    assert f"CALL {mock_procedure_name}(:username, :password);" in executed_call

