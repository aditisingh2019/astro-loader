"""
Purpose:
--------
Validate incoming data against structural and business rules.

Responsibilities:
-----------------
- Verify required columns exist.
- Validate data types and null constraints.
- Identify invalid records without stopping the pipeline.

Important Behavior:
-------------------
- Splits data into valid and rejected records.
- Adds a rejection reason to invalid records.
- Does not mutate valid data values.

Design Notes:
-------------
- Validation rules are config-driven.
- Designed to support schema evolution.
"""

from __future__ import annotations

import logging
import pandas as pd
from typing import Tuple, Dict, List, Any

logger = logging.getLogger(__name__)


# Validation Configuration

VALIDATION_CONFIG: Dict[str, Any] = {
    "required_columns": [
        "Booking ID",
        "Customer ID",
        "Vehicle Type",
        "Booking Status",
        "Date",
        "Time"
    ],
    "completed_required_fields": [
        "Booking Value",
        "Ride Distance"
    ],
    "rating_range": (1.0, 5.0),
    "non_negative_fields": [
        "Booking Value",
        "Ride Distance",
        "Avg VTAT",
        "Avg CTAT"
    ]
}


# Validate dataframe and split into valid and rejected records.

def validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    logger.info("Starting validation process.")

    df = df.copy()

    # Structural validation
    _validate_required_columns(df)

    # Row-level validation
    reject_mask = pd.Series(False, index=df.index)
    reject_reasons: Dict[int, List[str]] = {}

    for rule in [
        _validate_required_not_null,
        _validate_completed_fields,
        _validate_rating_range,
        _validate_non_negative_values
    ]:
        mask, reasons = rule(df)
        reject_mask |= mask
        for idx, reason in reasons.items():
            reject_reasons.setdefault(idx, []).append(reason)

    # Split dataset
    valid_df = df.loc[~reject_mask].copy()
    reject_df = df.loc[reject_mask].copy()

    if not reject_df.empty:
        reject_df["reject_reason"] = reject_df.index.map(
            lambda idx: "; ".join(reject_reasons.get(idx, []))
        )

    logger.info(
        f"Validation complete. "
        f"Valid rows: {len(valid_df)}, Rejected rows: {len(reject_df)}"
    )

    return valid_df, reject_df


# Structural Validation
def _validate_required_columns(df: pd.DataFrame) -> None:
    required = VALIDATION_CONFIG["required_columns"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(f"Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    logger.info("All required columns present.")


# Row-Level Rules
def _validate_required_not_null(df: pd.DataFrame):
    mask = pd.Series(False, index=df.index)
    reasons = {}

    for col in VALIDATION_CONFIG["required_columns"]:
        null_rows = df[col].isna()
        for idx in df[null_rows].index:
            reasons[idx] = f"{col} is NULL"
        mask |= null_rows

    return mask, reasons


def _validate_completed_fields(df: pd.DataFrame):
    mask = pd.Series(False, index=df.index)
    reasons = {}

    completed_mask = df["Booking Status"].str.strip().str.lower() == "completed"

    for field in VALIDATION_CONFIG["completed_required_fields"]:
        if field not in df.columns:
            continue

        invalid_rows = completed_mask & df[field].isna()

        for idx in df[invalid_rows].index:
            reasons[idx] = f"{field} required for completed rides"

        mask |= invalid_rows

    return mask, reasons


def _validate_rating_range(df: pd.DataFrame):
    mask = pd.Series(False, index=df.index)
    reasons = {}

    min_rating, max_rating = VALIDATION_CONFIG["rating_range"]

    for col in ["Driver Ratings", "Customer Rating"]:
        if col not in df.columns:
            continue

        invalid_rows = (
            df[col].notna() &
            ((df[col] < min_rating) | (df[col] > max_rating))
        )

        for idx in df[invalid_rows].index:
            reasons[idx] = f"{col} outside allowed range {min_rating}-{max_rating}"

        mask |= invalid_rows

    return mask, reasons


def _validate_non_negative_values(df: pd.DataFrame):
    mask = pd.Series(False, index=df.index)
    reasons = {}

    for col in VALIDATION_CONFIG["non_negative_fields"]:
        if col not in df.columns:
            continue

        invalid_rows = df[col].notna() & (df[col] < 0)

        for idx in df[invalid_rows].index:
            reasons[idx] = f"{col} cannot be negative"

        mask |= invalid_rows

    return mask, reasons
