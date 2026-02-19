"""
Purpose:
--------
Validate incoming data against structural and business rules.

Responsibilities:
-----------------
- Verify required columns exist.
- Validate data types and null constraints.
- Identify invalid records without stopping the pipeline.
- Ensure cancellation and incomplete event consistency.

Important Behavior:
-------------------
- Splits data into valid and rejected records.
- Adds a rejection reason to invalid records.
- Does not mutate valid data values.
"""

from __future__ import annotations

import logging
import pandas as pd
from typing import Tuple, Dict, List, Any

logger = logging.getLogger(__name__)


# ==============================
# Validation Configuration
# ==============================

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


# ==============================
# Main Validation Entry
# ==============================

def validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    logger.info("Starting validation process.")
    df = df.copy()

    _validate_required_columns(df)

    reject_mask = pd.Series(False, index=df.index)
    reject_reasons: Dict[int, List[str]] = {}

    for rule in [
        _validate_required_not_null,
        _validate_completed_fields,
        _validate_rating_range,
        _validate_non_negative_values,
        _validate_event_consistency,
    ]:
        mask, reasons = rule(df)
        reject_mask |= mask

        for idx, reason_list in reasons.items():
            reject_reasons.setdefault(idx, []).extend(reason_list)

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


# ==============================
# Structural Validation
# ==============================

def _validate_required_columns(df: pd.DataFrame) -> None:
    required = VALIDATION_CONFIG["required_columns"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(f"Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    logger.info("All required columns present.")


# ==============================
# Row-Level Validation Rules
# ==============================

def _validate_required_not_null(df: pd.DataFrame):
    mask = pd.Series(False, index=df.index)
    reasons = {}

    for col in VALIDATION_CONFIG["required_columns"]:
        null_rows = df[col].isna()

        for idx in df[null_rows].index:
            reasons.setdefault(idx, []).append(f"{col} is NULL")

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
            reasons.setdefault(idx, []).append(
                f"{field} required for completed rides"
            )

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
            df[col].notna()
            & ((df[col] < min_rating) | (df[col] > max_rating))
        )

        for idx in df[invalid_rows].index:
            reasons.setdefault(idx, []).append(
                f"{col} outside allowed range {min_rating}-{max_rating}"
            )

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
            reasons.setdefault(idx, []).append(
                f"{col} cannot be negative"
            )

        mask |= invalid_rows

    return mask, reasons


# ==============================
# Event Consistency Validation
# ==============================

def _validate_event_consistency(df: pd.DataFrame):
    mask = pd.Series(False, index=df.index)
    reasons = {}

    cust_flag = df.get("Cancelled Rides by Customer")
    cust_reason = df.get("Reason for cancelling by Customer")

    drv_flag = df.get("Cancelled Rides by Driver")
    drv_reason = df.get("Driver Cancellation Reason")

    inc_flag = df.get("Incomplete Rides")

    for idx in df.index:

        customer_cancel = (
            cust_flag is not None
            and idx in cust_flag.index
            and cust_flag.loc[idx] == 1
        )

        driver_cancel = (
            drv_flag is not None
            and idx in drv_flag.index
            and drv_flag.loc[idx] == 1
        )

        incomplete = (
            inc_flag is not None
            and idx in inc_flag.index
            and inc_flag.loc[idx] == 1
        )

        # ---- Dual cancellation ----
        if customer_cancel and driver_cancel:
            reasons.setdefault(idx, []).append(
                "Both customer and driver cancellation flags set"
            )
            mask.loc[idx] = True

        # ---- Reason without flag ----
        if (
            not customer_cancel
            and cust_reason is not None
            and idx in cust_reason.index
            and pd.notna(cust_reason.loc[idx])
        ):
            reasons.setdefault(idx, []).append(
                "Customer cancellation reason provided but flag not set"
            )
            mask.loc[idx] = True

        if (
            not driver_cancel
            and drv_reason is not None
            and idx in drv_reason.index
            and pd.notna(drv_reason.loc[idx])
        ):
            reasons.setdefault(idx, []).append(
                "Driver cancellation reason provided but flag not set"
            )
            mask.loc[idx] = True

        # ---- Mutually exclusive ----
        if (customer_cancel or driver_cancel) and incomplete:
            reasons.setdefault(idx, []).append(
                "Ride cannot be both cancelled and incomplete"
            )
            mask.loc[idx] = True

    return mask, reasons
