/*
Purpose:
--------
Define database schema for staging and reject tables.

Responsibilities:
-----------------
- Create staging tables for valid ingested data.
- Create reject tables for invalid or failed records.
- Enforce basic constraints where appropriate.

Important Behavior:
-------------------
- Staging tables prioritize load speed over constraints.
- Reject tables include an error_reason column.
- Schema changes should be backward compatible.

Design Notes:
-------------
- Tables follow stg_* naming convention.
- Designed for analytics-ready ingestion layers.
*/
