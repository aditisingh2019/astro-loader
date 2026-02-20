import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sqlalchemy import select, func, desc, cast, Float
from src.db.connection import get_engine
from src.db.tables import (
    bookings_table,
    booking_status_table,
    vehicle_types_table,
    locations_table,
    cancellations_table,
    cancellation_reasons_table,
    incomplete_rides_table,
)

logger = logging.getLogger(__name__)


def uber_analysis():
    engine = get_engine()

    try:
        with engine.begin() as connection:

            # Revenue Analysis
            booking_value = cast(bookings_table.c.booking_value, Float)
            clean_booking_value = cast(
                func.nullif(bookings_table.c.booking_value, 'NaN'),
                Float
            )

            # Total revenue
            query = select(func.coalesce(func.sum(clean_booking_value), 0))
            result = connection.execute(query).scalar()
            print(f"Total revenue for 2024: {result:.2f}")

            # Revenue by month
            query = select(
                            func.extract('month', bookings_table.c.booking_date).label("month"),
                            func.to_char(bookings_table.c.booking_date, 'FMMonth').label("month_name"),
                            func.coalesce(func.sum(clean_booking_value), 0).label("Revenue")
                        ).group_by(
                            func.extract('month', bookings_table.c.booking_date),
                            func.to_char(bookings_table.c.booking_date, 'FMMonth')
                        )

            df = pd.read_sql(query, con=connection)
            df["revenue"] = df["Revenue"].astype(float)

            print("Total revenue by month:")
            for _, row in df.iterrows():
                print(f"{row['month_name']}  {row['Revenue']:.2f}")

            df.plot.line(x='month_name', y='Revenue', marker='o')
            plt.title("Total Revenue by Month")
            plt.grid(visible=True)
            plt.xlabel("Month")
            plt.ylabel("Total Revenue (in millions of Rupees)")
            plt.xticks(range(12), labels=df['month_name'], rotation=45,  ha='right')
            plt.show()

            # Revenue by Vehicle Type
            query = (
                select(
                    vehicle_types_table.c.vehicle_type_name,
                    func.sum(booking_value).label("revenue"),
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
            print("\nTotal revenue by vehicle type:")
            for vehicle, revenue in result:
                print(f"{vehicle}: {revenue:.2f}")

            # Revenue by Pickup Location (Top 25)
            query = (
                select(
                    locations_table.c.location_name,
                    func.sum(booking_value).label("revenue"),
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
            print("\nTop 25 pickup locations by revenue:")
            for location, revenue in result:
                print(f"{location}: {revenue:.2f}")

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
            )

            df = pd.read_sql(query, con=connection).set_index("status_name")
            df.plot.pie(
                y="count",
                autopct="%1.1f%%",
                legend=False,
                ylabel="",
            )
            plt.legend(title="Booking Status", labels=df.index)
            plt.title("Booking Status Distribution")
            plt.tight_layout()
            plt.show()


            # Cancellation Analysis

            # Total cancellations
            query = select(func.count(cancellations_table.c.booking_id))
            total_cancellations = connection.execute(query).scalar()
            print(f"\nTotal number of cancellations: {total_cancellations}")

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
            print(
                f"Estimated lost revenue from cancellations: "
                f"{estimated_lost_revenue:.2f}"
            )

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
            )

            df = pd.read_sql(query, con=connection)
            if not df.empty:
                plt.figure(figsize=(7, 7))
                plt.pie(
                    df["count"],
                    labels=df["reason_description"],
                    autopct="%1.1f%%",
                    startangle=90,
                )
                plt.title("Customer Ride Cancellations by Reason")
                plt.axis("equal")
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
            )

            df = pd.read_sql(query, con=connection)
            if not df.empty:
                plt.figure(figsize=(7, 7))
                plt.pie(
                    df["count"],
                    labels=df["reason_description"],
                    autopct="%1.1f%%",
                    startangle=90,
                )
                plt.title("Driver Ride Cancellations by Reason")
                plt.axis("equal")
                plt.tight_layout()
                plt.show()

            # Incomplete Ride Analysis
            query = select(func.count(incomplete_rides_table.c.booking_id))
            total_incomplete = connection.execute(query).scalar()
            print(f"\nTotal incomplete rides: {total_incomplete}")

            query = (
                select(func.coalesce(func.sum(booking_value), 0))
                .join(
                    incomplete_rides_table,
                    bookings_table.c.booking_id
                    == incomplete_rides_table.c.booking_id,
                )
            )

            incomplete_cost = connection.execute(query).scalar()
            print(f"Total cost of incomplete rides: {incomplete_cost:.2f}")
            
            # Correlation Analysis
            numeric_cols = [
                bookings_table.c.booking_value,
                bookings_table.c.ride_distance,
                bookings_table.c.avg_vtat,
                bookings_table.c.avg_ctat,
                bookings_table.c.customer_rating,
            ]

            query = select(*numeric_cols)
            df_numeric = pd.read_sql(query, con=connection)
            corr = df_numeric.corr()

            print("\nCorrelation Matrix:")
            print(corr)

            plt.figure(figsize=(8, 6))
            sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm")
            plt.title("Correlation Matrix of Booking Metrics")
            plt.tight_layout()
            plt.show()

            # Distance Buckets
            bins = [0, 5, 10, 20, 50, 100, 500]
            labels = ["0-5", "5-10", "10-20", "20-50", "50-100", "100+"]

            df_numeric["distance_bucket"] = pd.cut(
                df_numeric["ride_distance"],
                bins=bins,
                labels=labels,
                right=False,
            )

            bucket_counts = df_numeric.groupby("distance_bucket").size()
            print("\nNumber of rides by distance bucket:")
            print(bucket_counts)

            bucket_counts.plot.bar()
            plt.title("Number of Rides by Distance Bucket")
            plt.xlabel("Distance Bucket (km)")
            plt.ylabel("Number of Rides")
            plt.tight_layout()
            plt.show()

            # Correlation by Vehicle Type
            query = (
                select(
                    bookings_table.c.booking_value,
                    bookings_table.c.ride_distance,
                    bookings_table.c.avg_vtat,
                    bookings_table.c.avg_ctat,
                    bookings_table.c.customer_rating,
                    vehicle_types_table.c.vehicle_type_name,
                )
                .join(
                    vehicle_types_table,
                    bookings_table.c.vehicle_type_id
                    == vehicle_types_table.c.vehicle_type_id,
                )
            )

            df_vehicle = pd.read_sql(query, con=connection)

            for vehicle in df_vehicle["vehicle_type_name"].unique():
                subset = (
                    df_vehicle[df_vehicle["vehicle_type_name"] == vehicle]
                    .drop(columns=["vehicle_type_name"])
                )

                corr = subset.corr()
                print(f"\nCorrelation matrix for vehicle type: {vehicle}")
                print(corr)

                plt.figure(figsize=(6, 5))
                sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm")
                plt.title(f"Correlation Matrix for {vehicle}")
                plt.tight_layout()
                plt.show()

    except Exception:
        logger.exception("Failed to run Uber analysis.")
        raise


if __name__ == "__main__":
    uber_analysis()
