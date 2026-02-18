/*
Purpose:
--------
Define STAGING database schema.

Responsibilities:
-----------------
- Hold validated, deduplicated ingested data.
- Serve as the source for ELT transformations.
- Store rejected records for observability.

Design Notes:
-------------
- Tables prefixed with stg_.
- Light constraints, FK integrity allowed.
- Append-friendly.
*/


CREATE TABLE IF NOT EXISTS error_log (
    log_id      BIGSERIAL PRIMARY KEY,
    asctime     TIMESTAMP NOT NULL,
    levelname   VARCHAR(20) NOT NULL,
    module      VARCHAR(100),
    lineno      INTEGER,
    message     TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_error_log_asctime
    ON error_log(asctime);

CREATE INDEX IF NOT EXISTS idx_error_log_level
    ON error_log(levelname);

CREATE TABLE IF NOT EXISTS data_log (
    log_id      BIGSERIAL PRIMARY KEY,
    asctime     TIMESTAMP NOT NULL,
    levelname   VARCHAR(20) NOT NULL,
    module      VARCHAR(100),
    lineno      INTEGER,
    message     TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_data_log_asctime
    ON data_log(asctime);

CREATE INDEX IF NOT EXISTS idx_data_log_level
    ON data_log(levelname);



CREATE TABLE IF NOT EXISTS stg_rejects (
    reject_id     BIGSERIAL PRIMARY KEY,
    source_name   TEXT NOT NULL,
    raw_record    JSONB NOT NULL,
    reject_reason TEXT NOT NULL,
    ingestion_ts  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS stg_rides (
    booking_id VARCHAR(20),
    booking_date DATE,
    booking_time TIME,

    booking_status TEXT,
    customer_id VARCHAR(20),
    driver_id INTEGER,
    vehicle_type TEXT,

    pickup_location TEXT,
    drop_location TEXT,

    avg_vtat DECIMAL(6,2),
    avg_ctat DECIMAL(6,2),
    booking_value DECIMAL(10,2),
    ride_distance DECIMAL(6,2),

    customer_rating DECIMAL(3,2),
    payment_method TEXT,

    cancellation_reason TEXT,
    incomplete_reason TEXT,

    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);