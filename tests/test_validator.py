import pandas as pd
import pytest

from src.ingestion.validator import validate_dataframe

# Fixtures
@pytest.fixture
def base_valid_df():
    return pd.DataFrame({
        "Booking ID": ["B1", "B2"],
        "Customer ID": ["C1", "C2"],
        "Vehicle Type": ["Auto", "Car"],
        "Booking Status": ["Completed", "Completed"],
        "Date": ["2024-01-01", "2024-01-01"],
        "Time": ["10:00:00", "11:00:00"],
        "Booking Value": [100, 200],
        "Ride Distance": [5, 10],
        "Driver Ratings": [4.5, 5.0],
        "Customer Rating": [4.0, 4.8],
        "Avg VTAT": [2, 3],
        "Avg CTAT": [1, 2],
    })


# Structural Validation
def test_missing_required_columns():
    df = pd.DataFrame({"Booking ID": ["B1"]})

    with pytest.raises(ValueError):
        validate_dataframe(df)


# Null Validation
def test_required_null_rejected(base_valid_df):
    df = base_valid_df.copy()
    df.loc[0, "Customer ID"] = None

    valid, reject = validate_dataframe(df)

    assert len(valid) == 1
    assert len(reject) == 1
    assert "Customer ID is NULL" in reject.iloc[0]["reject_reason"]


# Completed Ride Rules
def test_completed_requires_value(base_valid_df):
    df = base_valid_df.copy()
    df.loc[0, "Booking Value"] = None

    valid, reject = validate_dataframe(df)

    assert len(reject) == 1
    assert "Booking Value required for completed rides" in reject.iloc[0]["reject_reason"]


# Rating Range Validation
def test_rating_out_of_range(base_valid_df):
    df = base_valid_df.copy()
    df.loc[0, "Driver Ratings"] = 7

    valid, reject = validate_dataframe(df)

    assert len(reject) == 1
    assert "Driver Ratings outside allowed range" in reject.iloc[0]["reject_reason"]


# Non-negative Validation
def test_negative_values_rejected(base_valid_df):
    df = base_valid_df.copy()
    df.loc[0, "Ride Distance"] = -5

    valid, reject = validate_dataframe(df)

    assert len(reject) == 1
    assert "Ride Distance cannot be negative" in reject.iloc[0]["reject_reason"]

# Multiple Failures Combined
def test_multiple_reject_reasons(base_valid_df):
    df = base_valid_df.copy()
    df.loc[0, "Ride Distance"] = -5
    df.loc[0, "Driver Ratings"] = 8

    valid, reject = validate_dataframe(df)

    reason = reject.iloc[0]["reject_reason"]

    assert "Ride Distance cannot be negative" in reason
    assert "Driver Ratings outside allowed range" in reason


# Valid Rows Pass Through
def test_valid_rows_unchanged(base_valid_df):
    valid, reject = validate_dataframe(base_valid_df)

    assert len(valid) == len(base_valid_df)
    assert reject.empty

# Ensure Input Not Mutated
def test_input_dataframe_not_mutated(base_valid_df):
    original = base_valid_df.copy(deep=True)

    validate_dataframe(base_valid_df)

    pd.testing.assert_frame_equal(original, base_valid_df)
