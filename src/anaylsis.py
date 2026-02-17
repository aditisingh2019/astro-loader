import pandas as pd
import numpy as np
import logging
import matplotlib.pyplot as plt
from src.db.connection import get_engine
from src.db.tables import bookings_table, booking_status_table, cancellations_table, customers_table, incomplete_rides_table, locations_table, vehicle_types_table
from sqlalchemy import select, func, or_, desc

logger = logging.getLogger(__name__)

def uber_analysis():

    engine = get_engine()

    try:
        with engine.begin() as connection:

            # Revenue analysis

            # Total revenue
            query = select(func.sum(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0)))
            result = connection.execute(query).scalar()
            print(f"Total revenue for 2024: {result:.2f}")

            # Total revenue by month
            query =  select(func.extract('month', bookings_table.c.booking_date).label("month"),
                            func.to_char(bookings_table.c.booking_date, 'FMMonth').label("month_name"),
                            func.sum(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0)).label("revenue")
                            ).group_by(
                                func.extract('month', bookings_table.c.booking_date),
                                func.to_char(bookings_table.c.booking_date, 'FMMonth').label("month_name"))
            
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
            query = select(vehicle_types_table.c.vehicle_type,
                        func.sum(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0))
                        ).join(
                            bookings_table,
                            vehicle_types_table.c.vehicle_type_id == bookings_table.c.vehicle_type_id
                            ).group_by(vehicle_types_table.c.vehicle_type
                                        ).order_by(
                                            func.sum(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0))
                                )

            result = connection.execute(query).all()
            print("Total revenue by vehicle type")
            for vehicle, total_revenue in result:
                print(f"{vehicle} : $ {total_revenue:.2f}")

            print("")

            # Total revenue contribution by pickup location (top 25)
            query = select(locations_table.c.location,
                        func.sum(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0))
                        ).join(
                            bookings_table,
                            locations_table.c.location_id == bookings_table.c.pickup_location_id
                        ).group_by(locations_table.c.location
                                    ).order_by(
                                        desc(func.sum(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0)))).limit(25)
            
            result = connection.execute(query).all()
            print("The top 25 grossing pickup locations:")
            for location, cost in result:
                print(f"{location} - total revenue: {cost:.2f}")

            print("")

            # Cancellation and Incomplete Ride analysis

            # Comparison of complete, incomplete, and cancelled rides
            query = select(booking_status_table.c.status, func.count(booking_status_table.c.status).label("count")
                        ).select_from(bookings_table.join(
                            booking_status_table, bookings_table.c.booking_status_id == booking_status_table.c.status_id)
                            ).group_by(
                                booking_status_table.c.status
                            )
            
            df = pd.read_sql(query, con=connection)
            df = df.set_index('status')
            df.plot.pie(autopct='%1.1f%%', labels=None, subplots=True)
            plt.legend(title="Booking Status", labels=df.index)
            plt.show()

            # Total cancellations
            query = select(func.count()
                        ).select_from(bookings_table.join(
                            booking_status_table, bookings_table.c.booking_status_id == booking_status_table.c.status_id)
                            ).where(
                                or_(booking_status_table.c.status == 'Cancelled By Driver', booking_status_table.c.status == 'Cancelled By Customer'))

            total_cancellations = connection.execute(query).scalar()
            print(f"Total number of cancellations: {total_cancellations}")
            
            # Estimated cost of cancellations (average ride cost)
            query = select(func.avg(func.coalesce(func.nullif(bookings_table.c.booking_value, 'NaN'), 0)))
            avg_cost_per_ride = connection.execute(query).scalar()
            print(f"Estimated lost revenue from cancellations: {(total_cancellations * avg_cost_per_ride):.2f}")

            # Total incomplete rides
            query = select(func.count(incomplete_rides_table.c.incomplete_ride_id))
            total_incomplete_rides = connection.execute(query).scalar()
            print(f"Total incomplete rides: {total_incomplete_rides}")

            # Total cost of incomplete rides
            query = select(func.sum(bookings_table.c.booking_value)
                        ).select_from(bookings_table.join(
                            incomplete_rides_table, bookings_table.c.booking_id == incomplete_rides_table.c.booking_id
                        ))
            total_cost = connection.execute(query).scalar()
            print(f"Total cost of incomplete rides {total_cost:.2f}")

            # Customer cancellations by reason
            query = select(cancellations_table.c.cancellation_reason).where(cancellations_table.c.cancelled_by == "customer")
            df = pd.read_sql(query, con=connection)
            df = df.groupby('cancellation_reason')['cancellation_reason'].count()
            df.plot.pie(autopct='%1.1f%%', labels=None)
            plt.title("Customer ride cancellations")
            plt.legend(title="Reason for cancellation", labels=df.index)
            plt.show()

            # Driver cancellations by reason
            query = select(cancellations_table.c.cancellation_reason).where(cancellations_table.c.cancelled_by == "driver")
            df = pd.read_sql(query, con=connection)
            df = df.groupby('cancellation_reason')['cancellation_reason'].count()
            df.plot.pie(autopct='%1.1f%%', labels=None)
            plt.title("Driver ride cancellations")
            plt.legend(title="Reason for cancellation", labels=df.index)
            plt.show()

            # Is there a correlation between location and cancelled rides?

    except Exception as e:
        logger.exception("Failed to query database for analysis.")
        raise