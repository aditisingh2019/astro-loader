"""
Purpose:
--------
Clean and standardize validated data before loading.

Responsibilities:
-----------------
- Normalize column names.
- Standardize data formats (dates, strings, numbers).
- Handle missing values safely.
- Keep transformations deterministic and reversible.

Important Behavior:
-------------------
- Only operates on valid records.
- Should not introduce new invalid states.
- Does NOT apply business logic.
"""

from __future__ import annotations

import logging
import pandas as pd
from typing import Dict

logger = logging.getLogger(__name__)


# =====================================================
# Column Normalization
# Must match stg_rides schema exactly
# =====================================================

COLUMN_RENAME_MAP: Dict[str, str] = {
    "Booking ID": "booking_id",
    "Customer ID": "customer_id",
    "Vehicle Type": "vehicle_type",
    "Pickup Location": "pickup_location",
    "Drop Location": "drop_location",
    "Booking Status": "booking_status",
    "Booking Value": "booking_value",
    "Ride Distance": "ride_distance",
    "Driver Ratings": "driver_ratings",
    "Customer Rating": "customer_rating",
    "Cancelled Rides by Customer": "cancelled_rides_by_customer",
    "Reason for Cancelling by Customer": "reason_for_cancelling_by_customer",
    "Cancelled Rides by Driver": "cancelled_rides_by_driver",
    "Driver Cancellation Reason": "driver_cancellation_reason",
    "Incomplete Rides": "incomplete_rides",
    "Incomplete Rides Reason": "incomplete_rides_reason",
    "Avg VTAT": "avg_vtat",
    "Avg CTAT": "avg_ctat",
    "Payment Method": "payment_method",
    "Date": "booking_date",
    "Time": "booking_time"
}


# =====================================================
# Public Cleaning Function
# =====================================================

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


# =====================================================
# Cleaning Steps
# =====================================================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizing column names to snake_case.")
    return df.rename(columns=COLUMN_RENAME_MAP)


def _strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Stripping whitespace from string columns.")

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
                .astype(str)
                .str.replace('"', '', regex=False)
                .str.strip()
            )

    return df


def _convert_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting numeric columns.")

    numeric_columns = [
        "booking_value",
        "ride_distance",
        "driver_ratings",
        "customer_rating",
        "avg_vtat",
        "avg_ctat",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _convert_binary_flags(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Converting cancellation/incomplete flags to integers (0/1).")

    flag_columns = [
        "cancelled_rides_by_customer",
        "cancelled_rides_by_driver",
        "incomplete_rides",
    ]

    for col in flag_columns:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .fillna(0)
                .astype(int)
            )

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

    return df



def _standardize_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Standardizing categorical text fields.")

    categorical_columns = [
        "vehicle_type",
        "pickup_location",
        "drop_location",
        "booking_status",
        "payment_method",
        "reason_for_cancelling_by_customer",
        "driver_cancellation_reason",
        "incomplete_rides_reason"
    ]

    for col in categorical_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace("nan", pd.NA)
                .str.title()
            )

    return df
