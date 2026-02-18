"""
Purpose:
--------
Clean and standardize validated data before loading.

Responsibilities:
-----------------
- Normalize column names.
- Standardize data formats (dates, strings, numbers).
- Handle missing values where appropriate.

Important Behavior:
-------------------
- Only operates on valid records.
- Should not introduce new invalid states.
- Keeps transformations predictable and reversible.

Design Notes:
-------------
- Focused on light transformations, not business logic.
- Designed to be composable and testable.
"""

from __future__ import annotations

import logging
import pandas as pd
import numpy as np
from typing import Dict

logger = logging.getLogger(__name__)


# Column Normalization
COLUMN_RENAME_MAP: Dict[str, str] = {
    "Booking ID": "booking_id",
    "Customer ID": "customer_id",
    "Vehicle Type": "vehicle_type",
    "Pickup Location": "pickup_location",
    "Drop Location": "drop_location",
    "Booking Status": "booking_status",
    "Booking Value": "booking_value",
    "Ride Distance": "ride_distance",
    "Driver Ratings": "driver_rating",
    "Customer Rating": "customer_rating",
    "Cancelled Rides by Customer": "cancelled_by_customer",
    "Reason for cancelling by Customer": "cancellation_reason_by_customer",
    "Cancelled Rides by Driver": "cancelled_by_driver",
    "Driver Cancellation Reason": "cancellation_reason_by_driver",
    "Incomplete Rides": "incomplete_ride",
    "Incomplete Rides Reason": "incomplete_ride_reason",
    "Avg VTAT": "avg_vtat",
    "Avg CTAT": "avg_ctat",
    "Payment Method": "payment_method",
    "Date": "booking_date",
    "Time": "booking_time"
}


# Apply deterministic, reversible cleaning transformations.
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    


    logger.info("Starting cleaning process.")

    df = df.copy()

    df = _normalize_columns(df)
    df = _strip_whitespace(df)
    df = _standardize_ids(df)
    df = _convert_numeric_types(df)
    df = _convert_datetime(df)
    df = _standardize_categoricals(df)
    df = _convert_binary_flags(df)

    logger.info(f"Cleaning complete. Final shape: {df.shape}")

    return df



# Cleaning Steps
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizing column names to snake_case.")
    df = df.rename(columns=COLUMN_RENAME_MAP)
    return df


def _strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Stripping leading/trailing whitespace from string columns.")
    str_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    return df


def _standardize_ids(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Standardizing ID columns.")

    id_columns = ["booking_id", "customer_id"]

    for col in id_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .str.replace('"', '', regex=False)
                .str.strip()
            )

    return df


def _convert_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting numeric columns to appropriate types.")

    numeric_columns = [
        "booking_value",
        "ride_distance",
        "driver_rating",
        "customer_rating",
        "avg_vtat",
        "avg_ctat",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _convert_binary_flags(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting cancellation/incomplete flags to integers.")

    flag_columns = [
        "cancelled_by_customer",
        "cancelled_by_driver",
        "incomplete_ride",
    ]

    for col in flag_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype(int)

    return df


def _convert_datetime(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting date and time columns.")

    if "booking_date" in df.columns:
        df["booking_date"] = pd.to_datetime(
            df["booking_date"],
            format="%Y-%m-%d",
            errors="coerce"
        ).dt.date

    if "booking_time" in df.columns:
        df["booking_time"] = pd.to_datetime(
            df["booking_time"],
            format="%H:%M:%S",
            errors="coerce"
        ).dt.time

    # Create combined timestamp
    if "booking_date" in df.columns and "booking_time" in df.columns:
        logger.info("Creating ride_timestamp column.")
        df["ride_timestamp"] = pd.to_datetime(
            df["booking_date"].astype(str) + " " + df["booking_time"].astype(str),
            errors="coerce"
        )

    return df


def _standardize_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Standardizing categorical columns.")

    categorical_columns = [
        "vehicle_type",
        "pickup_location",
        "drop_location",
        "booking_status",
        "payment_method",
        "cancellation_reason_by_customer",
        "cancellation_reason_by_driver",
        "incomplete_ride_reason"
    ]

    for col in categorical_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .str.strip()
                .str.title()
            )

    return df
