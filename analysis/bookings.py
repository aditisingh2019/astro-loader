"""
Booking Status & General Insights Module.
- Analyze booking status distribution
- Provide overview of booking patterns
"""

import logging
import pandas as pd
import matplotlib.pyplot as plt

from sqlalchemy import select, func
from src.db.tables import (
    bookings_table,
    booking_status_table,
)

logger = logging.getLogger(__name__)


def analyze_bookings(connection) -> None:
    """Analyze booking status distribution and patterns."""
    
    logger.info("Starting booking analysis")
    
    try:
        print(f"\n{'='*60}")
        print(f"BOOKING INSIGHTS")
        print(f"{'='*60}\n")
        
        # Total bookings
        query = select(func.count(bookings_table.c.booking_id))
        total_bookings = connection.execute(query).scalar()
        print(f"Total bookings in database: {total_bookings:,}")

        # Booking Status Distribution
        query = (
            select(
                booking_status_table.c.status_name,
                func.count(bookings_table.c.booking_id).label("count"),
            )
            .join(
                bookings_table,
                bookings_table.c.status_id == booking_status_table.c.status_id,
            )
            .group_by(booking_status_table.c.status_name)
            .order_by(func.count(bookings_table.c.booking_id).desc())
        )

        df = pd.read_sql(query, con=connection)
        
        print(f"\nBooking Status Distribution:")
        print("-" * 50)
        for _, row in df.iterrows():
            pct = (row['count'] / df['count'].sum()) * 100
            print(f"  {row['status_name']:<25} {row['count']:>10,} ({pct:>5.1f}%)")

        if len(df) > 0:
            df.plot.pie(
                x='status_name',
                y='count',
                autopct="%1.1f%%",
                legend=False,
                ylabel="",
                figsize=(8, 6)
            )
            plt.title("Booking Status Distribution")
            plt.tight_layout()
            plt.show()

        logger.info("Booking analysis completed successfully")

    except Exception as e:
        logger.exception("Failed to run booking analysis")
        raise
