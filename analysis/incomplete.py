"""
Incomplete Rides Analysis Module.
- Analyze incomplete ride patterns
- Calculate cost of incomplete rides
- Track reasons for incomplete rides
"""

import logging
import pandas as pd

from sqlalchemy import select, func, cast, Float
from src.db.tables import (
    bookings_table,
    incomplete_rides_table,
)

logger = logging.getLogger(__name__)


def analyze_incomplete_rides(connection) -> None:
    """Analyze incomplete ride patterns and impact."""
    
    logger.info("Starting incomplete rides analysis")
    
    try:
        booking_value = cast(bookings_table.c.booking_value, Float)
        
        print(f"\n{'='*60}")
        print(f"INCOMPLETE RIDES ANALYSIS")
        print(f"{'='*60}\n")
        
        # Total incomplete rides
        query = select(func.count(incomplete_rides_table.c.booking_id))
        total_incomplete = connection.execute(query).scalar()
        print(f"Total incomplete rides: {total_incomplete:,}")

        # Total cost of incomplete rides
        query = (
            select(func.coalesce(func.sum(booking_value), 0))
            .select_from(incomplete_rides_table)
            .join(
                bookings_table,
                bookings_table.c.booking_id
                == incomplete_rides_table.c.booking_id,
            )
        )

        incomplete_cost = connection.execute(query).scalar()
        print(f"Total cost of incomplete rides: ₹{incomplete_cost:,.2f}")
        
        if total_incomplete > 0:
            avg_incomplete_cost = incomplete_cost / total_incomplete
            print(f"Average incomplete ride value: ₹{avg_incomplete_cost:,.2f}")

        logger.info("Incomplete rides analysis completed successfully")

    except Exception as e:
        logger.exception("Failed to run incomplete rides analysis")
        raise
