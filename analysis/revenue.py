import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import select, func, cast, Float, desc
from sqlalchemy.engine import Engine

from src.db.tables import (
    bookings_table,
    vehicle_types_table,
    locations_table,
)


def run_revenue_analysis(engine: Engine) -> None:
    with engine.begin() as connection:

        booking_value = cast(bookings_table.c.booking_value, Float)

        # Total revenue
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
                        func.coalesce(func.sum(clean_booking_value), 0).label("revenue")
                    ).group_by(
                        func.extract('month', bookings_table.c.booking_date),
                        func.to_char(bookings_table.c.booking_date, 'FMMonth')
                    )

        df = pd.read_sql(query, con=connection)
        df["revenue"] = df["revenue"].astype(float)

        print("Total revenue by month:")
        for _, row in df.iterrows():
            print(f"{row['month_name']}  {row['revenue']:.2f}")

        df.plot.line(x="month_name", y="revenue", marker="o")
        plt.title("Total Revenue by Month")
        plt.xlabel("Month")
        plt.ylabel("Total Revenue")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()

        # Revenue by vehicle type
        query = select(
                    vehicle_types_table.c.vehicle_type_name,
                    func.sum(func.coalesce(clean_booking_value)).label("revenue")
                ).join(
                    bookings_table,
                    vehicle_types_table.c.vehicle_type_id == bookings_table.c.vehicle_type_id
                ).group_by(vehicle_types_table.c.vehicle_type_name
                ).order_by(func.sum(func.coalesce(bookings_table.c.booking_value, 0)).desc())

        df = pd.read_sql(query, connection)
        print("\nRevenue by vehicle type:")
        print(df)
