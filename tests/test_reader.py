import pytest
import pandas as pd
from src.ingestion import reader

mock_data = pd.DataFrame({
    'id' : [1, 2, 3, 4],
    'name' : ['Gamma', 'Beta', 'Omega', 'Lambda']
})

def test_read_file_success(tmp_path):
    # Create a temporary file path name
    csv_file_path = tmp_path / "test_data.csv"
    
    # Combine the mock data with the temporary file path name
    mock_data.to_csv(csv_file_path, index=False)

    # Test the function call
    result_df = reader.read_file(csv_file_path)

    # Assert the results
    assert result_df['name'].iloc[0] == 'Gamma'

def test_read_file_filenotfounderror_success():

    with pytest.raises(Exception) as e:
        reader.read_file("t.xtt")
