"""
Cancellation Analysis Module.
- Analyze cancellation patterns and reasons
- Calculate lost revenue from cancellations
- Breakdown by customer vs. driver cancellations
"""

import logging
import pandas as pd
import matplotlib.pyplot as plt

from sqlalchemy import select, func, cast, Float
from src.db.tables import (
    bookings_table,
    booking_status_table,
    cancellations_table,
    cancellation_reasons_table,
)

logger = logging.getLogger(__name__)


def analyze_cancellations(connection) -> None:
    """Analyze cancellation patterns and impact on revenue."""
    
    logger.info("Starting cancellation analysis")
    
    try:
        booking_value = cast(bookings_table.c.booking_value, Float)
        
        # Total cancellations
        query = select(func.count(cancellations_table.c.booking_id))
        total_cancellations = connection.execute(query).scalar()
        
        print(f"\n{'='*60}")
        print(f"CANCELLATION ANALYSIS")
        print(f"{'='*60}\n")
        print(f"Total number of cancellations: {total_cancellations:,}")

        # Avg completed ride value (for lost revenue estimation)
        avg_completed_value_query = (
            select(func.avg(booking_value))
            .join(
                booking_status_table,
                bookings_table.c.status_id == booking_status_table.c.status_id,
            )
            .where(booking_status_table.c.status_name == "Completed")
        )

        avg_completed_value = (
            connection.execute(avg_completed_value_query).scalar() or 0
        )

        estimated_lost_revenue = total_cancellations * avg_completed_value
        print(f"Average completed ride value: ₹{avg_completed_value:,.2f}")
        print(f"Estimated lost revenue from cancellations: ₹{estimated_lost_revenue:,.2f}")

        # Customer cancellations by reason
        query = (
            select(
                cancellation_reasons_table.c.reason_description,
                func.count(cancellations_table.c.booking_id).label("count"),
            )
            .join(
                cancellations_table,
                cancellations_table.c.reason_id
                == cancellation_reasons_table.c.reason_id,
            )
            .where(cancellations_table.c.cancelled_by == "CUSTOMER")
            .group_by(cancellation_reasons_table.c.reason_description)
            .order_by(func.count(cancellations_table.c.booking_id).desc())
        )

        df_customer = pd.read_sql(query, con=connection)
        
        if not df_customer.empty:
            print(f"\nCustomer Cancellations by Reason:")
            print("-" * 50)
            for _, row in df_customer.iterrows():
                pct = (row['count'] / df_customer['count'].sum()) * 100
                print(f"  {row['reason_description']:<35} {row['count']:>6,} ({pct:>5.1f}%)")
            
            plt.figure(figsize=(10, 6))
            plt.pie(
                df_customer["count"],
                labels=df_customer["reason_description"],
                autopct="%1.1f%%",
                startangle=90,
            )
            plt.title("Customer Ride Cancellations by Reason")
            plt.tight_layout()
            plt.show()

        # Driver cancellations by reason
        query = (
            select(
                cancellation_reasons_table.c.reason_description,
                func.count(cancellations_table.c.booking_id).label("count"),
            )
            .join(
                cancellations_table,
                cancellations_table.c.reason_id
                == cancellation_reasons_table.c.reason_id,
            )
            .where(cancellations_table.c.cancelled_by == "DRIVER")
            .group_by(cancellation_reasons_table.c.reason_description)
            .order_by(func.count(cancellations_table.c.booking_id).desc())
        )

        df_driver = pd.read_sql(query, con=connection)
        
        if not df_driver.empty:
            print(f"\nDriver Cancellations by Reason:")
            print("-" * 50)
            for _, row in df_driver.iterrows():
                pct = (row['count'] / df_driver['count'].sum()) * 100
                print(f"  {row['reason_description']:<35} {row['count']:>6,} ({pct:>5.1f}%)")
            
            plt.figure(figsize=(10, 6))
            plt.pie(
                df_driver["count"],
                labels=df_driver["reason_description"],
                autopct="%1.1f%%",
                startangle=90,
            )
            plt.title("Driver Ride Cancellations by Reason")
            plt.tight_layout()
            plt.show()

        logger.info("Cancellation analysis completed successfully")

    except Exception as e:
        logger.exception("Failed to run cancellation analysis")
        raise
