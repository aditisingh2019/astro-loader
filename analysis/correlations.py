import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import select
from sqlalchemy.engine import Engine

from src.db.tables import bookings_table, vehicle_types_table


def run_correlation_analysis(engine: Engine) -> None:
    with engine.begin() as connection:

        query = select(
            bookings_table.c.booking_value,
            bookings_table.c.ride_distance,
            bookings_table.c.avg_vtat,
            bookings_table.c.avg_ctat,
            bookings_table.c.customer_rating,
        )

        df = pd.read_sql(query, connection)
        corr = df.corr()

        print("\nCorrelation matrix:")
        print(corr)

        sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm")
        plt.title("Correlation Matrix")
        plt.show()

        # Correlation by vehicle type
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

        df = pd.read_sql(query, connection)

        for vt in df["vehicle_type_name"].unique():
            subset = df[df["vehicle_type_name"] == vt].drop(
                columns=["vehicle_type_name"]
            )
            sns.heatmap(subset.corr(), annot=True, fmt=".2f")
            plt.title(f"Correlation – {vt}")
            plt.show()
