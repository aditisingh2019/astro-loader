import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import select, func
from sqlalchemy.engine import Engine

from src.db.tables import (
    bookings_table,
    booking_status_table,
    cancellations_table,
    cancellation_reasons_table,
    incomplete_rides_table,
)


def run_cancellation_analysis(engine: Engine) -> None:
    with engine.begin() as connection:

        # Total cancellations
        total_cancellations = connection.execute(
            select(func.count(cancellations_table.c.booking_id))
        ).scalar()

        print(f"\nTotal cancellations: {total_cancellations}")

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

        df = pd.read_sql(query, connection)
        if not df.empty:
            plt.figure(figsize=(7, 7))
            plt.pie(df["count"], labels=df["reason_description"], autopct="%1.1f%%")
            plt.title("Customer Cancellations by Reason")
            plt.show()

        # Incomplete rides
        incomplete_count = connection.execute(
            select(func.count(incomplete_rides_table.c.booking_id))
        ).scalar()

        print(f"Total incomplete rides: {incomplete_count}")
