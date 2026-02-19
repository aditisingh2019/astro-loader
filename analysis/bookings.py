import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import select, func

from src.db.tables import bookings_table, booking_status_table


def run_booking_status_analysis(connection) -> None:
    query = (
        select(
            booking_status_table.c.status_name,
            func.count(bookings_table.c.booking_id).label("count"),
        )
        .join(bookings_table)
        .group_by(booking_status_table.c.status_name)
    )

    df = pd.read_sql(query, connection).set_index("status_name")
    df.plot.pie(y="count", autopct="%1.1f%%", legend=False, ylabel="")
    plt.legend(title="Booking Status", labels=df.index)
    plt.title("Booking Status Distribution")
    plt.tight_layout()
    plt.show()


def run_distance_bucket_analysis(connection) -> None:
    query = select(
        bookings_table.c.ride_distance
    )

    df = pd.read_sql(query, connection)

    bins = [0, 5, 10, 20, 50, 100, 500]
    labels = ["0-5", "5-10", "10-20", "20-50", "50-100", "100+"]

    df["distance_bucket"] = pd.cut(
        df["ride_distance"],
        bins=bins,
        labels=labels,
        right=False,
    )

    counts = df.groupby("distance_bucket").size()

    print("\nNumber of rides by distance bucket:")
    print(counts)

    counts.plot.bar()
    plt.title("Number of Rides by Distance Bucket")
    plt.tight_layout()
    plt.show()

