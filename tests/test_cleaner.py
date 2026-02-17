import pandas as pd
import pytest
from datetime import date, time

from src.ingestion.cleaner import clean_dataframe


# Fixtures
@pytest.fixture
def raw_df():
    return pd.DataFrame({
        "Booking ID": [' "B1" ', '"B2"'],
        "Customer ID": [' "C1" ', '"C2"'],
        "Vehicle Type": [" auto ", "CAR"],
        "Pickup Location": [" noida ", "gurgaon"],
        "Drop Location": [" delhi ", "Noida"],
        "Booking Status": [" completed ", "CANCELLED"],
        "Booking Value": ["100", "bad_value"],
        "Ride Distance": ["5", "10"],
        "Driver Ratings": ["4.5", "5.0"],
        "Customer Rating": ["4.0", "invalid"],
        "Cancelled Rides by Customer": [None, 1],
        "Reason for cancelling by Customer": [None, None],
        "Cancelled Rides by Driver": [None, None],
        "Driver Cancellation Reason": [None, None],
        "Incomplete Rides": [None, None],
        "Incomplete Rides Reason": [None, None],
        "Avg VTAT": ["2", "-1"],
        "Avg CTAT": ["1", "3"],
        "Payment Method": [" upi ", "CASH"],
        "Date": ["2024-01-01", "2024-01-02"],
        "Time": ["10:00:00", "11:00:00"],
    })


# Column Normalization
def test_columns_are_renamed(raw_df):
    cleaned = clean_dataframe(raw_df)

    assert "booking_id" in cleaned.columns
    assert "customer_id" in cleaned.columns
    assert "ride_date" in cleaned.columns
    assert "ride_time" in cleaned.columns


# Whitespace + ID Cleaning
def test_id_quotes_removed_and_stripped(raw_df):
    cleaned = clean_dataframe(raw_df)

    assert cleaned.loc[0, "booking_id"] == "B1"
    assert cleaned.loc[0, "customer_id"] == "C1"


# Numeric Conversion
def test_numeric_conversion_and_coercion(raw_df):
    cleaned = clean_dataframe(raw_df)

    # Valid numeric conversion
    assert cleaned.loc[0, "booking_value"] == 100.0

    # Invalid numeric coerced to NaN
    assert pd.isna(cleaned.loc[1, "booking_value"])

    # Invalid customer rating coerced
    assert pd.isna(cleaned.loc[1, "customer_rating"])


# Date & Time Conversion
def test_date_and_time_conversion(raw_df):
    cleaned = clean_dataframe(raw_df)

    assert isinstance(cleaned.loc[0, "ride_date"], date)
    assert isinstance(cleaned.loc[0, "ride_time"], time)

    # Combined timestamp exists
    assert "ride_timestamp" in cleaned.columns
    assert pd.notna(cleaned.loc[0, "ride_timestamp"])


# Categorical Standardization
def test_categorical_standardization(raw_df):
    cleaned = clean_dataframe(raw_df)

    assert cleaned.loc[0, "vehicle_type"] == "Auto"
    assert cleaned.loc[0, "pickup_location"] == "Noida"
    assert cleaned.loc[1, "vehicle_type"] == "Car"
    assert cleaned.loc[1, "payment_method"] == "Cash"


# Binary Flag Conversion
def test_binary_flags_converted_to_int(raw_df):
    cleaned = clean_dataframe(raw_df)

    assert cleaned.loc[0, "cancelled_by_customer"] == 0
    assert cleaned.loc[1, "cancelled_by_customer"] == 1
    assert cleaned.loc[0, "cancelled_by_driver"] == 0
    assert cleaned.loc[0, "incomplete_ride"] == 0

    assert cleaned["cancelled_by_customer"].dtype == "int64"


# Shape Preservation
def test_row_count_preserved(raw_df):
    cleaned = clean_dataframe(raw_df)

    assert len(cleaned) == len(raw_df)


# Input Not Mutated
def test_input_dataframe_not_mutated(raw_df):
    original = raw_df.copy(deep=True)

    clean_dataframe(raw_df)

    pd.testing.assert_frame_equal(original, raw_df)
