import pytest
import pandas as pd
from src.ingestion import deduplicator

@pytest.fixture
def empty_set():
    return set()

@pytest.fixture
def dataframe_with_duplicates():
    return pd.DataFrame({
        'booking_id' : [1, 2, 3, 4, 2, 5, 3],
        'name' : ['Gamma', 'Beta', 'Omega', 'Lambda', 'Beta', 'Epsilon', 'Omega']
    })

@pytest.fixture
def dataframe_with_cross_duplicates():
    return pd.DataFrame({
        'booking_id' : [2, 6, 7, 8, 3],
        'name' : ['Beta', 'Mu', 'Pi', 'Rho', 'Omega']
    })

@pytest.fixture
def dataframe_without_duplicates():
    return pd.DataFrame({
        'booking_id' : [9, 10, 11, 12, 13],
        'name' : ['Kappa', 'Chi', 'Iota', 'Zeta', 'Delta']
    })

@pytest.fixture
def dataframe_with_all_data():
        return pd.DataFrame({
            'booking_id' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
            'name' : ['Gamma', 'Beta', 'Omega', 'Lambda', 'Epsilon', 'Mu', 'Pi', 'Rho', 'Kappa', 'Chi', 'Iota', 'Zeta', 'Delta']
    })

@pytest.fixture
def dataframe_list(dataframe_with_duplicates, dataframe_with_cross_duplicates, dataframe_without_duplicates):
    df_list = []
    df_list.append(dataframe_with_duplicates)
    df_list.append(dataframe_with_cross_duplicates)
    df_list.append(dataframe_without_duplicates)
    return df_list

# Duplicates removed from single batch of data
def test_deduplicator_across_single_batch_success(dataframe_with_duplicates, empty_set):

    df = pd.DataFrame(dataframe_with_duplicates)
    df.drop_duplicates(inplace=True)

    result_df = deduplicator.deduplicate(dataframe_with_duplicates, empty_set)

    assert df.equals(result_df)

# Duplicates removed from different batches  of same data sourse
def test_dedupliator_across_multiple_batches_success(dataframe_list, dataframe_with_all_data, empty_set):

    result = pd.DataFrame()

    for chunk in dataframe_list:
        df = deduplicator.deduplicate(chunk, empty_set)
        result = pd.concat([result, df], ignore_index=True)

    assert dataframe_with_all_data.equals(result)
    