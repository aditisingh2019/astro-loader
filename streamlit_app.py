import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from src.db.connection import get_engine
from sqlalchemy import text

st.set_page_config(page_title="Uber Rides Analytics", layout="wide")


@st.cache_resource
def get_engine_cached():
    return get_engine()


@st.cache_data(ttl=300)
def load_bookings(start_date=None, end_date=None, vehicle_types=None, statuses=None):
    base_query = """
    SELECT
      b.booking_date::date AS booking_date,
      b.booking_value::float AS booking_value,
      b.ride_distance::float AS ride_distance,
      b.avg_vtat::float AS avg_vtat,
      b.avg_ctat::float AS avg_ctat,
      b.customer_rating::float AS customer_rating,
      vt.vehicle_type_name AS vehicle_type,
      bs.status_name AS booking_status
    FROM bookings b
    LEFT JOIN vehicle_types vt ON b.vehicle_type_id = vt.vehicle_type_id
    LEFT JOIN booking_statuses bs ON b.status_id = bs.status_id
    WHERE 1=1
    """

    filters = []
    params = {}

    if start_date is not None:
        filters.append("AND b.booking_date >= %(start_date)s")
        params["start_date"] = str(start_date)
    if end_date is not None:
        filters.append("AND b.booking_date <= %(end_date)s")
        params["end_date"] = str(end_date)
    if vehicle_types:
        filters.append("AND vt.vehicle_type_name = ANY(%(vehicle_types)s)")
        params["vehicle_types"] = list(vehicle_types)
    if statuses:
        filters.append("AND bs.status_name = ANY(%(statuses)s)")
        params["statuses"] = list(statuses)

    if filters:
        base_query = base_query + "\n" + "\n".join(filters)

    engine = get_engine_cached()
    df = pd.read_sql(base_query, con=engine, params=params)

    if not df.empty:
        df["booking_date"] = pd.to_datetime(df["booking_date"])
    return df


def main():
    st.title("Uber Rides Analytics")

    engine = get_engine_cached()

    # Get global min/max dates for filters
    with engine.begin() as conn:
        try:
            min_date = pd.to_datetime(conn.execute(text("SELECT MIN(booking_date) FROM bookings")).scalar())
            max_date = pd.to_datetime(conn.execute(text("SELECT MAX(booking_date) FROM bookings")).scalar())
        except Exception:
            min_date = None
            max_date = None

    st.sidebar.header("Filters")
    if min_date is not None and max_date is not None:
        date_range = st.sidebar.date_input("Booking date range", value=(min_date.date(), max_date.date()))
        start_date, end_date = date_range
    else:
        start_date = None
        end_date = None

    # Vehicle types and statuses
    with engine.begin() as conn:
        vehicle_types = [r[0] for r in conn.execute(text("SELECT vehicle_type_name FROM vehicle_types ORDER BY vehicle_type_name")).fetchall()]
        statuses = [r[0] for r in conn.execute(text("SELECT status_name FROM booking_statuses ORDER BY status_name")).fetchall()]

    selected_vehicle_types = st.sidebar.multiselect("Vehicle types", options=vehicle_types)
    selected_statuses = st.sidebar.multiselect("Booking status", options=statuses)

    df = load_bookings(start_date=start_date, end_date=end_date, vehicle_types=selected_vehicle_types or None, statuses=selected_statuses or None)

    st.header("Overview")
    col1, col2, col3 = st.columns(3)
    total_bookings = int(len(df))
    total_revenue = float(df["booking_value"].sum()) if not df.empty else 0.0
    avg_booking = float(df["booking_value"].mean()) if not df.empty else 0.0

    col1.metric("Total bookings", f"{total_bookings:,}")
    col2.metric("Total revenue", f"₹{total_revenue:,.2f}")
    col3.metric("Avg booking value", f"₹{avg_booking:,.2f}")

    st.markdown("---")

    # Revenue by month
    if not df.empty:
        revenue_month = df.set_index("booking_date").resample("M")["booking_value"].sum()
        st.subheader("Revenue by month")
        st.line_chart(revenue_month)

        # Booking status distribution
        st.subheader("Booking status distribution")
        status_counts = df["booking_status"].value_counts()
        fig1, ax1 = plt.subplots()
        ax1.pie(status_counts.values, labels=status_counts.index, autopct="%1.1f%%", startangle=90)
        ax1.axis("equal")
        st.pyplot(fig1)

        # Revenue by vehicle type
        st.subheader("Revenue by vehicle type")
        vt_rev = df.groupby("vehicle_type")["booking_value"].sum().sort_values(ascending=False)
        st.bar_chart(vt_rev)

        # Correlation heatmap (numeric columns)
        st.subheader("Correlation matrix")
        numeric = df[["booking_value", "ride_distance", "avg_vtat", "avg_ctat", "customer_rating"]].dropna()
        if not numeric.empty:
            corr = numeric.corr()
            fig2, ax2 = plt.subplots(figsize=(6, 5))
            sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", ax=ax2)
            st.pyplot(fig2)

    # Raw data viewer
    st.markdown("---")
    st.subheader("Sample data")
    if df.empty:
        st.info("No data for selected filters")
    else:
        st.dataframe(df.head(500))


if __name__ == "__main__":
    main()
