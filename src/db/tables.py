"""
Purpose:
--------
Provide a centralized location for table schema.

Responsibilities:
-----------------
- Configuration of the table, columns, and datatypes.
- Allows for access to table information via SQLAlchemy MetaData object.

Important Behavior:
-------------------
- Tables must reflect the exact schema in database.
- Can utilize the MetaData object to create table in database, if needed.

Design Notes:
-------------
- Uses SQLAlchemy library to create MetaData instances that reflect the database tables.
"""
from sqlalchemy import Table, Column, Integer, String, MetaData

metadata = MetaData()

error_log_table = Table(
    'error_log',
    metadata,
    Column("error_log_id", Integer, primary_key=True),
    Column("asctime", String(30)),
    Column("levelname", String(20)),
    Column("module", String(30)),
    Column("lineno", Integer),
    Column("message", String(50)),
)

data_log_table = Table(
    'data_log',
    metadata,
    Column("data_log_id", Integer, primary_key=True),
    Column("asctime", String(30)),
    Column("levelname", String(20)),
    Column("module", String(30)),
    Column("lineno", Integer),
    Column("message", String(50)),
)
