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

CREATE TABLE IF NOT EXISTS stg_meteorites (
    meteorite_id        INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,
    name_type           TEXT,
    rec_class           TEXT,
    mass_grams          DOUBLE PRECISION,
    fall                TEXT,
    year                DATE,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,

    ingestion_ts        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_meteorites_year
ON stg_meteorites (year);

CREATE INDEX idx_meteorites_fall
ON stg_meteorites (fall);

CREATE INDEX idx_meteorites_location
ON stg_meteorites (latitude, longitude);

CREATE TABLE IF NOT EXISTS stg_rejects (
    reject_id           BIGSERIAL PRIMARY KEY,
    source_name         TEXT NOT NULL,
    raw_record          JSONB NOT NULL,
    reject_reason       TEXT NOT NULL,

    ingestion_ts        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
