import pytest
import pandas as pd
import json
from src.ingestion import reader

@pytest.fixture
def mock_dataframe():
    return pd.DataFrame({
    'booking_id' : [1, 2, 3, 4],
    'name' : ['Gamma', 'Beta', 'Omega', 'Lambda']
    })

@pytest.fixture
def chunk_size():
    return 2

# Read csv file
def test_read_csv_file_success(tmp_path, mock_dataframe, chunk_size):
    # Create a temporary file path name
    csv_file_path = tmp_path / "test_data.csv"
    
    # Combine the mock data with the temporary file path name
    mock_dataframe.to_csv(csv_file_path, index=False)

    # Test the function call
    df = reader.read_file(csv_file_path, chunk_size)
    result_df = pd.DataFrame()

    for chunk in df:
        result_df = pd.concat([result_df, chunk], ignore_index=True)
    
    # Assert the results
    assert result_df.equals(mock_dataframe)

# Test file not found error
def test_read_file_filenotfounderror_success(chunk_size):

    with pytest.raises(Exception) as e:
        reader.read_file("t.csv", chunk_size)

    assert str(e.value) == "File not found"

# Test file extension error
def test_read_file_extension_error_success(tmp_path, mock_dataframe, chunk_size):
    # Create a temporary file path name
    csv_file_path = tmp_path / "test_data.xxx"

    # Combine the mock data with the temporary file path name
    mock_dataframe.to_csv(csv_file_path, index=False)

    with pytest.raises(ValueError) as e:
        reader.read_file(csv_file_path, chunk_size)

    assert str(e.value) == "File extension .xxx not supported"