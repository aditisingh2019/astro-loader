"""
Revenue Analysis Module.
- Analyze total and monthly revenue
- Revenue breakdown by vehicle type and pickup location
"""

import logging
import pandas as pd
import matplotlib.pyplot as plt

from sqlalchemy import select, func, desc, cast, Float
from src.db.tables import (
    bookings_table,
    vehicle_types_table,
    locations_table,
)

logger = logging.getLogger(__name__)


def analyze_revenue(connection) -> None:
    """Analyze revenue patterns from booking data."""
    
    logger.info("Starting revenue analysis")
    
    try:
        # Total revenue
        booking_value = cast(bookings_table.c.booking_value, Float)
        clean_booking_value = cast(
            func.nullif(bookings_table.c.booking_value, 'NaN'),
            Float
        )

        query = select(func.coalesce(func.sum(clean_booking_value), 0))
        total_revenue = connection.execute(query).scalar()
        print(f"\n{'='*60}")
        print(f"Total revenue for 2024: ₹{total_revenue:,.2f}")
        print(f"{'='*60}\n")

        # Revenue by month
        query = select(
            func.extract('month', bookings_table.c.booking_date).label("month"),
            func.to_char(bookings_table.c.booking_date, 'FMMonth').label("month_name"),
            func.coalesce(func.sum(clean_booking_value), 0).label("Revenue")
        ).group_by(
            func.extract('month', bookings_table.c.booking_date),
            func.to_char(bookings_table.c.booking_date, 'FMMonth')
        ).order_by(func.extract('month', bookings_table.c.booking_date))

        df = pd.read_sql(query, con=connection)
        df["revenue"] = df["Revenue"].astype(float)

        print("Revenue by Month:")
        print("-" * 40)
        for _, row in df.iterrows():
            print(f"{row['month_name']:12} ₹{row['Revenue']:>12,.2f}")

        if len(df) > 1:
            df.plot.line(x='month_name', y='Revenue', marker='o', figsize=(10, 6))
            plt.title("Total Revenue by Month (2024)")
            plt.grid(visible=True, alpha=0.3)
            plt.xlabel("Month")
            plt.ylabel("Revenue (₹)")
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.show()

        # Revenue by Vehicle Type
        query = (
            select(
                vehicle_types_table.c.vehicle_type_name,
                func.sum(booking_value).label("revenue"),
                func.count(bookings_table.c.booking_id).label("ride_count"),
            )
            .join(
                bookings_table,
                bookings_table.c.vehicle_type_id
                == vehicle_types_table.c.vehicle_type_id,
            )
            .group_by(vehicle_types_table.c.vehicle_type_name)
            .order_by(desc(func.sum(booking_value)))
        )

        result = connection.execute(query).all()
        print("\nRevenue by Vehicle Type:")
        print("-" * 60)
        print(f"{'Vehicle Type':<20} {'Revenue':>15} {'Rides':>10}")
        print("-" * 60)
        for vehicle, revenue, ride_count in result:
            print(f"{vehicle:<20} ₹{revenue:>13,.2f} {ride_count:>10,}")

        # Revenue by Pickup Location (Top 25)
        query = (
            select(
                locations_table.c.location_name,
                func.sum(booking_value).label("revenue"),
                func.count(bookings_table.c.booking_id).label("ride_count"),
            )
            .join(
                bookings_table,
                bookings_table.c.pickup_location_id
                == locations_table.c.location_id,
            )
            .group_by(locations_table.c.location_name)
            .order_by(desc(func.sum(booking_value)))
            .limit(25)
        )

        result = connection.execute(query).all()
        print("\nTop 25 Pickup Locations by Revenue:")
        print("-" * 60)
        print(f"{'Location':<30} {'Revenue':>15} {'Rides':>10}")
        print("-" * 60)
        for location, revenue, ride_count in result:
            print(f"{location:<30} ₹{revenue:>13,.2f} {ride_count:>10,}")

        logger.info("Revenue analysis completed successfully")

    except Exception as e:
        logger.exception("Failed to run revenue analysis")
        raise
