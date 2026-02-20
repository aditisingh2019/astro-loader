"""
Correlation Analysis Module.
- Analyze correlations between booking metrics
- Breakdown correlations by vehicle type
- Identify distance patterns and relationships
"""

import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sqlalchemy import select
from src.db.tables import (
    bookings_table,
    vehicle_types_table,
)

logger = logging.getLogger(__name__)


def analyze_correlations(connection) -> None:
    """Analyze correlations between booking metrics."""
    
    logger.info("Starting correlation analysis")
    
    try:
        print(f"\n{'='*60}")
        print(f"CORRELATION ANALYSIS")
        print(f"{'='*60}\n")
        
        # Overall Correlation Matrix
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

        print("Overall Correlation Matrix:")
        print("-" * 60)
        print(corr.to_string())

        plt.figure(figsize=(8, 6))
        sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", cbar_kws={'label': 'Correlation'})
        plt.title("Correlation Matrix of Booking Metrics")
        plt.tight_layout()
        plt.show()

        # Analyze distance buckets
        analyze_distance_buckets(df_numeric)

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

        print("\n" + "="*60)
        print("Correlation Analysis by Vehicle Type:")
        print("="*60)

        for vehicle in sorted(df_vehicle["vehicle_type_name"].unique()):
            subset = (
                df_vehicle[df_vehicle["vehicle_type_name"] == vehicle]
                .drop(columns=["vehicle_type_name"])
            )

            corr = subset.corr()
            print(f"\n{vehicle}:")
            print("-" * 60)
            print(corr.to_string())

            plt.figure(figsize=(8, 6))
            sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", cbar_kws={'label': 'Correlation'})
            plt.title(f"Correlation Matrix for {vehicle}")
            plt.tight_layout()
            plt.show()

        logger.info("Correlation analysis completed successfully")

    except Exception as e:
        logger.exception("Failed to run correlation analysis")
        raise


def analyze_distance_buckets(df_numeric: pd.DataFrame) -> None:
    """Analyze ride distribution by distance buckets."""
    
    bins = [0, 5, 10, 20, 50, 100, 500]
    labels = ["0-5km", "5-10km", "10-20km", "20-50km", "50-100km", "100+km"]

    df_numeric["distance_bucket"] = pd.cut(
        df_numeric["ride_distance"],
        bins=bins,
        labels=labels,
        right=False,
    )

    bucket_counts = df_numeric.groupby("distance_bucket", observed=True).size()
    
    print("\nRide Distribution by Distance Bucket:")
    print("-" * 60)
    for bucket, count in bucket_counts.items():
        pct = (count / bucket_counts.sum()) * 100
        print(f"  {bucket:<12} {count:>10,} ({pct:>5.1f}%)")

    bucket_counts.plot.bar(figsize=(10, 6), color='steelblue', edgecolor='black')
    plt.title("Number of Rides by Distance Bucket")
    plt.xlabel("Distance Bucket")
    plt.ylabel("Number of Rides")
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.show()
