"""
Purpose:
--------
Provide a centralized location for table access.

Responsibilities:
-----------------
- Loads tables from database using SQLAlchemy engine.
- Allows for access to table information via SQLAlchemy MetaData object.

Important Behavior:
-------------------
- Using SQLAlchemy reflection ensures column names and datatype match the database.
- Can utilize the MetaData object to create table in database, if needed.

Design Notes:
-------------
- Uses SQLAlchemy library to create MetaData instances that reflect the database tables.
- Do not have to explicitly define the column name and datatype as that information is pulled from the database itself.
"""

from src.db.connection import get_engine
from sqlalchemy import Table, MetaData

# Object to hold table data
metadata = MetaData()
# Obtain connection to the database
engine = get_engine()

error_log_table = Table("error_log", metadata, autoload_with=engine)
data_log_table = Table("data_log", metadata, autoload_with=engine)

stg_rejects_table = Table("stg_rejects", metadata, autoload_with=engine)

stg_bookings_table = Table("stg_bookings", metadata, autoload_with=engine)
stg_booking_status_table = Table("stg_booking_status", metadata, autoload_with=engine)
stg_vehicle_types_table = Table("stg_vehicle_types", metadata, autoload_with=engine)
stg_locations_table = Table("stg_locations", metadata, autoload_with=engine)
stg_customers_table = Table("stg_customers", metadata, autoload_with=engine)
stg_drivers_table = Table("stg_drivers", metadata, autoload_with=engine)
stg_cancellations_table = Table("stg_cancellations", metadata, autoload_with=engine)
stg_incomplete_rides_table = Table("stg_incomplete_rides", metadata, autoload_with=engine)
