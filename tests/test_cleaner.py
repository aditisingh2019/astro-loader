import pytest
import pandas as pd
import numpy as np
import datetime as dt

from src.ingestion.cleaner import clean_dataframe


def make_raw_dataframe():
    return pd.DataFrame({
        "Booking ID": [' "123" '],
        "Customer ID": [' 456 '],
        "Vehicle Type": ["  sedan "],
        "Pickup Location": [" downtown "],
        "Drop Location": [" airport "],
        "Booking Status": [" completed "],
        "Booking Value": ["100.5"],
        "Ride Distance": ["12.3"],
        "Driver Ratings": ["4.8"],
        "Customer Rating": ["bad_rating"],
        "Cancelled Rides by Customer": [None],
        "Cancelled Rides by Driver": [1],
        "Incomplete Rides": [None],
        "Avg VTAT": ["5"],
        "Avg CTAT": ["3"],
        "Payment Method": [" cash "],
        "Date": ["2024-01-01"],
        "Time": ["10:30:00"],
    })


def test_full_cleaning_pipeline_transforms_correctly():
    df = make_raw_dataframe()
    original = df.copy(deep=True)

    cleaned = clean_dataframe(df)

    # input is not mutated
    pd.testing.assert_frame_equal(df, original)

    # renamed columns
    assert "booking_id" in cleaned.columns
    assert "customer_id" in cleaned.columns
    assert "booking_date" in cleaned.columns
    assert "booking_time" in cleaned.columns

    # ID cleanup
    assert cleaned.loc[0, "booking_id"] == "123"
    assert cleaned.loc[0, "customer_id"] == "456"

    # numeric coercion
    assert cleaned.loc[0, "booking_value"] == 100.5
    assert isinstance(cleaned.loc[0, "booking_value"], float)
    assert np.isnan(cleaned.loc[0, "customer_rating"])

    # datetime coercion
    assert isinstance(cleaned.loc[0, "booking_date"], dt.date)
    assert cleaned.loc[0, "booking_date"] == dt.date(2024, 1, 1)

    assert isinstance(cleaned.loc[0, "booking_time"], dt.time)
    assert cleaned.loc[0, "booking_time"] == dt.time(10, 30, 0)

    # categoricals standardized
    assert cleaned.loc[0, "vehicle_type"] == "Sedan"
    assert cleaned.loc[0, "booking_status"] == "Completed"
    assert cleaned.loc[0, "payment_method"] == "Cash"

    # binary flags
    assert cleaned.loc[0, "cancelled_rides_by_customer"] == 0
    assert cleaned.loc[0, "cancelled_rides_by_driver"] == 1
    assert cleaned.loc[0, "incomplete_rides"] == 0


def test_missing_optional_columns_do_not_break_cleaning():
    df = pd.DataFrame({
        "Booking ID": ["1"],
        "Customer ID": ["2"],
        "Date": ["2024-01-01"],
        "Time": ["10:00:00"],
    })

    cleaned = clean_dataframe(df)

    assert "booking_id" in cleaned.columns
    assert "booking_date" in cleaned.columns
    assert "booking_time" in cleaned.columns


def test_invalid_datetime_values_coerce_to_null():
    df = pd.DataFrame({
        "Booking ID": ["1"],
        "Customer ID": ["2"],
        "Date": ["invalid-date"],
        "Time": ["invalid-time"],
    })

    cleaned = clean_dataframe(df)

    assert pd.isna(cleaned.loc[0, "booking_date"])
    assert pd.isna(cleaned.loc[0, "booking_time"])


def test_strip_whitespace_applies_to_all_object_columns():
    df = pd.DataFrame({
        "Booking ID": [" 1 "],
        "Customer ID": [" 2 "],
        "Vehicle Type": ["  bike  "],
    })

    cleaned = clean_dataframe(df)

    assert cleaned.loc[0, "booking_id"] == "1"
    assert cleaned.loc[0, "customer_id"] == "2"
    assert cleaned.loc[0, "vehicle_type"] == "Bike"


def test_numeric_coercion_handles_invalid_strings():
    df = pd.DataFrame({
        "Booking ID": ["1"],
        "Customer ID": ["2"],
        "Booking Value": ["not_a_number"],
    })

    cleaned = clean_dataframe(df)

    assert np.isnan(cleaned.loc[0, "booking_value"])


def test_binary_flags_fillna_and_cast_to_int():
    df = pd.DataFrame({
        "Booking ID": ["1"],
        "Customer ID": ["2"],
        "Cancelled Rides by Customer": [None],
        "Cancelled Rides by Driver": [0],
        "Incomplete Rides": [None],
    })

    cleaned = clean_dataframe(df)

    assert cleaned.loc[0, "cancelled_rides_by_customer"] == 0
    assert cleaned.loc[0, "cancelled_rides_by_driver"] == 0
    assert cleaned.loc[0, "incomplete_rides"] == 0
    assert cleaned["cancelled_rides_by_customer"].dtype == int
