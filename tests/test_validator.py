import pytest
import pandas as pd

from src.ingestion.validator import (
    validate_dataframe,
    _validate_required_columns,
)


def make_base_dataframe():
    return pd.DataFrame({
        "Booking ID": [1, 2],
        "Customer ID": [10, 20],
        "Vehicle Type": ["Car", "Bike"],
        "Booking Status": ["Completed", "Pending"],
        "Date": ["2024-01-01", "2024-01-02"],
        "Time": ["10:00", "11:00"],
        "Booking Value": [100.0, 50.0],
        "Ride Distance": [10.0, 5.0],
        "Avg VTAT": [5.0, 3.0],
        "Avg CTAT": [4.0, 2.0],
        "Driver Ratings": [4.5, 3.5],
        "Customer Rating": [4.0, 5.0],
    })

def test_missing_required_columns_raises():
    df = pd.DataFrame({
        "Booking ID": [1],
        "Customer ID": [1],
    })

    with pytest.raises(ValueError) as exc:
        _validate_required_columns(df)

    assert "Missing required columns" in str(exc.value)


def test_all_required_columns_present_does_not_raise():
    df = make_base_dataframe()
    _validate_required_columns(df)



def test_fully_valid_dataframe_returns_all_valid():
    df = make_base_dataframe()
    original = df.copy(deep=True)

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == len(df)
    assert reject_df.empty

    pd.testing.assert_frame_equal(valid_df.reset_index(drop=True),
                                  original.reset_index(drop=True))


def test_required_not_null_rejects_rows():
    df = make_base_dataframe()
    df.loc[0, "Customer ID"] = None

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == 1
    assert len(reject_df) == 1
    assert "Customer ID is NULL" in reject_df.iloc[0]["reject_reason"]


def test_completed_requires_booking_value_and_distance():
    df = make_base_dataframe()
    df.loc[0, "Booking Status"] = "Completed"
    df.loc[0, "Booking Value"] = None
    df.loc[0, "Ride Distance"] = None

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == 1
    assert len(reject_df) == 1
    reason = reject_df.iloc[0]["reject_reason"]

    assert "Ride Distance required for completed rides" in reason


def test_completed_check_is_case_and_whitespace_insensitive():
    df = make_base_dataframe()
    df.loc[0, "Booking Status"] = "  COMPLETED  "
    df.loc[0, "Booking Value"] = None

    valid_df, reject_df = validate_dataframe(df)

    assert len(reject_df) == 1
    assert "Booking Value required for completed rides" in reject_df.iloc[0]["reject_reason"]


def test_rating_outside_allowed_range_rejected():
    df = make_base_dataframe()
    df.loc[0, "Driver Ratings"] = 6.0
    df.loc[1, "Customer Rating"] = 0.5

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == 0
    assert len(reject_df) == 2

    reasons = reject_df["reject_reason"].tolist()
    assert any("Driver Ratings outside allowed range" in r for r in reasons)
    assert any("Customer Rating outside allowed range" in r for r in reasons)


def test_rating_columns_missing_are_ignored():
    df = make_base_dataframe()
    df = df.drop(columns=["Driver Ratings", "Customer Rating"])

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == len(df)
    assert reject_df.empty


def test_negative_values_rejected():
    df = make_base_dataframe()
    df.loc[0, "Booking Value"] = -10
    df.loc[1, "Avg VTAT"] = -1

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == 0
    assert len(reject_df) == 2

    reasons = reject_df["reject_reason"].tolist()
    assert any("Booking Value cannot be negative" in r for r in reasons)
    assert any("Avg VTAT cannot be negative" in r for r in reasons)


def test_non_negative_fields_missing_are_ignored():
    df = make_base_dataframe()
    df = df.drop(columns=["Avg VTAT", "Avg CTAT"])

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == len(df)
    assert reject_df.empty


def test_multiple_rules_accumulate_reject_reasons():
    df = make_base_dataframe()
    df.loc[0, "Customer ID"] = None
    df.loc[0, "Booking Value"] = -100
    df.loc[0, "Driver Ratings"] = 10

    valid_df, reject_df = validate_dataframe(df)

    assert len(valid_df) == 1
    assert len(reject_df) == 1

    reason = reject_df.iloc[0]["reject_reason"]

    assert "Customer ID is NULL" in reason
    assert "Booking Value cannot be negative" in reason
    assert "Driver Ratings outside allowed range" in reason


def test_reject_dataframe_contains_reject_reason_column():
    df = make_base_dataframe()
    df.loc[0, "Customer ID"] = None

    _, reject_df = validate_dataframe(df)

    assert "reject_reason" in reject_df.columns
    assert isinstance(reject_df.iloc[0]["reject_reason"], str)
