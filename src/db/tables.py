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

stg_rides_table = Table("stg_rides", metadata, autoload_with=engine)
stg_rejects_table = Table("stg_rejects", metadata, autoload_with=engine)


booking_status_table = Table("booking_statuses", metadata, autoload_with=engine)
vehicle_types_table = Table("vehicle_types", metadata, autoload_with=engine)
locations_table = Table("locations", metadata, autoload_with=engine)
payment_methods_table = Table("payment_methods", metadata, autoload_with=engine)
cancellation_reasons_table = Table("cancellation_reasons", metadata, autoload_with=engine)
incomplete_reasons_table = Table("incomplete_reasons", metadata, autoload_with=engine)

customers_table = Table("customers", metadata, autoload_with=engine)

bookings_table = Table("bookings", metadata, autoload_with=engine)

cancellations_table = Table("cancellations", metadata, autoload_with=engine)
incomplete_rides_table = Table("incomplete_rides", metadata, autoload_with=engine)
