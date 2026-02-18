import pandas as pd
import numpy as np
import logging
import matplotlib.pyplot as plt
from src.db.connection import get_engine
from src.db.tables import (
    bookings_table, booking_status_table,
    customer_cancellations_table, driver_cancellations_table,
    incomplete_rides_table, customers_table,
    locations_table, vehicle_types_table,
    cancellation_reasons_table
)
from sqlalchemy import select, func, desc, join
import seaborn as sns

logger = logging.getLogger(__name__)

def uber_analysis():

    engine = get_engine()

    try:
        with engine.begin() as connection:

            # Revenue analysis

            # Total revenue
            query = select(func.sum(func.coalesce(bookings_table.c.booking_value, 0)))
            result = connection.execute(query).scalar()
            print(f"Total revenue for 2024: {result:.2f}")

            # Total revenue by month
            query = select(
                            func.extract('month', bookings_table.c.booking_date).label("month"),
                            func.to_char(bookings_table.c.booking_date, 'FMMonth').label("month_name"),
                            func.sum(func.coalesce(bookings_table.c.booking_value, 0)).label("revenue")
                        ).group_by(
                            func.extract('month', bookings_table.c.booking_date),
                            func.to_char(bookings_table.c.booking_date, 'FMMonth')
                        )
            
            result = connection.execute(query).all()
            print("Total revenue by month:")
            for row in result:
                print(f"{row.month_name}  {row.revenue:.2f}")

            df = pd.DataFrame(result)
            df['revenue'] = df['revenue'].astype(float)
            df.plot.line(x='month_name', y='revenue', marker='o')
            plt.title("Total revenue by month")
            plt.xlabel("Month")
            plt.ylabel("Total Revenue (in millions of Rupees)")
            plt.xticks(np.arange(0, 12, 1), labels=df['month_name'], rotation=45,  ha='right')
            plt.show()

            print("")

            # Total revenue contribution by vehicle type
            query = select(
                        vehicle_types_table.c.vehicle_type_name,
                        func.sum(func.coalesce(bookings_table.c.booking_value, 0)).label("revenue")
                    ).join(
                        bookings_table,
                        vehicle_types_table.c.vehicle_type_id == bookings_table.c.vehicle_type_id
                    ).group_by(vehicle_types_table.c.vehicle_type_name
                    ).order_by(func.sum(func.coalesce(bookings_table.c.booking_value, 0)).desc())

            result = connection.execute(query).all()
            print("Total revenue by vehicle type")
            for vehicle, total_revenue in result:
                print(f"{vehicle} : $ {total_revenue:.2f}")

            print("")

            # Total revenue contribution by pickup location (top 25)
            query = select(
                        locations_table.c.location_name,
                        func.sum(func.coalesce(bookings_table.c.booking_value, 0)).label("revenue")
                    ).join(
                        bookings_table,
                        locations_table.c.location_id == bookings_table.c.pickup_location_id
                    ).group_by(locations_table.c.location_name
                    ).order_by(desc(func.sum(func.coalesce(bookings_table.c.booking_value, 0)))).limit(25)
            result = connection.execute(query).all()
            print("The top 25 grossing pickup locations:")
            for location, cost in result:
                print(f"{location} - total revenue: {cost:.2f}")

            print("")

            # Cancellation and Incomplete Ride analysis

            # Comparison of complete, incomplete, and cancelled rides
            query = select(
                        booking_status_table.c.status_name,
                        func.count(bookings_table.c.booking_id).label("count")
                    ).join(
                        bookings_table,
                        bookings_table.c.status_id == booking_status_table.c.status_id
                    ).group_by(booking_status_table.c.status_name)
                    
            df = pd.read_sql(query, con=connection)
            df = df.set_index('status')
            df.plot.pie(autopct='%1.1f%%', labels=None, subplots=True)
            plt.legend(title="Booking Status", labels=df.index)
            plt.show()

            # Total cancellations
            query = select(func.count(bookings_table.c.booking_id)).join(
                        booking_status_table,
                        bookings_table.c.status_id == booking_status_table.c.status_id
                    ).where(
                        booking_status_table.c.status_name.in_(["Cancelled By Customer", "Cancelled By Driver"])
            )
            total_cancellations = connection.execute(query).scalar()
            print(f"Total number of cancellations: {total_cancellations}")
            
            # Estimated cost of cancellations (average ride cost)
            query = select(func.avg(func.coalesce(bookings_table.c.booking_value, 0)))
            avg_cost_per_ride = connection.execute(query).scalar()
            print(f"Estimated lost revenue from cancellations: {(total_cancellations * avg_cost_per_ride):.2f}")

            # Total incomplete rides
            query = select(func.count(incomplete_rides_table.c.booking_id))
            total_incomplete_rides = connection.execute(query).scalar()
            print(f"Total incomplete rides: {total_incomplete_rides}")

            # Total cost of incomplete rides
            query = select(func.sum(bookings_table.c.booking_value)).join(
                    incomplete_rides_table,
                    bookings_table.c.booking_id == incomplete_rides_table.c.booking_id
                )
            total_cost = connection.execute(query).scalar()
            print(f"Total cost of incomplete rides {total_cost:.2f}")

            # Customer cancellations by reason
            query = select(
                        cancellation_reasons_table.c.reason_description
                    ).join(
                        customer_cancellations_table,
                        cancellation_reasons_table.c.reason_id == customer_cancellations_table.c.reason_id
                    )
            df = pd.read_sql(query, con=connection)
            df = df.groupby('cancellation_reason')['cancellation_reason'].count()
            df.plot.pie(autopct='%1.1f%%', labels=None)
            plt.title("Customer ride cancellations")
            plt.legend(title="Reason for cancellation", labels=df.index)
            plt.show()

            # Driver cancellations by reason
            query = select(
                        cancellation_reasons_table.c.reason_description
                    ).join(
                        driver_cancellations_table,
                        cancellation_reasons_table.c.reason_id == driver_cancellations_table.c.reason_id
                    )
            df = pd.read_sql(query, con=connection)
            df = df.groupby('cancellation_reason')['cancellation_reason'].count()
            df.plot.pie(autopct='%1.1f%%', labels=None)
            plt.title("Driver ride cancellations")
            plt.legend(title="Reason for cancellation", labels=df.index)
            plt.show()

            # Is there a correlation between location and cancelled rides?







            # Full correlation matrix
            numeric_columns = [
                bookings_table.c.booking_value,
                bookings_table.c.ride_distance,
                bookings_table.c.avg_vtat,
                bookings_table.c.avg_ctat,
                bookings_table.c.customer_rating
            ]

            query = select(*numeric_columns)
            df_numeric = pd.read_sql(query, con=connection)
            corr_matrix = df_numeric.corr()
            print("Full correlation matrix:")
            print(corr_matrix)

            plt.figure(figsize=(8,6))
            plt.title("Correlation Matrix of Bookings")
            sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap="coolwarm")
            plt.show()

            # Number of rides by distance buckets
            distance_bins = [0, 5, 10, 20, 50, 100, 500]
            distance_labels = ['0-5', '5-10', '10-20', '20-50', '50-100', '100+']

            df_numeric['distance_bucket'] = pd.cut(df_numeric['ride_distance'], bins=distance_bins, labels=distance_labels, right=False)
            rides_by_bucket = df_numeric.groupby('distance_bucket').size()
            print("Number of rides by distance bucket:")
            print(rides_by_bucket)

            rides_by_bucket.plot.bar()
            plt.title("Number of Rides by Distance Bucket (km)")
            plt.xlabel("Distance Bucket")
            plt.ylabel("Number of Rides")
            plt.show()

            # Correlation by vehicle type
            query = select(
                bookings_table.c.booking_value,
                bookings_table.c.ride_distance,
                bookings_table.c.avg_vtat,
                bookings_table.c.avg_ctat,
                bookings_table.c.customer_rating,
                vehicle_types_table.c.vehicle_type_name
            ).join(
                vehicle_types_table,
                bookings_table.c.vehicle_type_id == vehicle_types_table.c.vehicle_type_id
            )

            df_vehicle = pd.read_sql(query, con=connection)

            vehicle_types = df_vehicle['vehicle_type_name'].unique()
            for vt in vehicle_types:
                df_vt = df_vehicle[df_vehicle['vehicle_type_name'] == vt].drop(columns=['vehicle_type_name'])
                corr = df_vt.corr()
                print(f"\nCorrelation matrix for vehicle type: {vt}")
                print(corr)
                
                plt.figure(figsize=(6,5))
                sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm")
                plt.title(f"Correlation Matrix for {vt}")
                plt.show()


    except Exception as e:
        logger.exception("Failed to query database for analysis.")
        raise