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
    
    booking_date DATE,
    booking_time TIME,

    booking_id VARCHAR(50),
    booking_status VARCHAR(50),
    
    customer_id VARCHAR(20),
    vehicle_type VARCHAR(50),

    pickup_location VARCHAR(255),
    drop_location VARCHAR(255),

    avg_vtat NUMERIC(10,2),
    avg_ctat NUMERIC(10,2),

    cancelled_rides_by_customer NUMERIC(10,2),
    reason_for_cancelling_by_customer TEXT,

    cancelled_rides_by_driver NUMERIC(10,2),
    driver_cancellation_reason TEXT,

    incomplete_rides NUMERIC(10,2),
    incomplete_rides_reason TEXT,

    booking_value NUMERIC(12,2),
    ride_distance NUMERIC(10,2),

    driver_ratings NUMERIC(3,2),
    customer_rating NUMERIC(3,2),

    payment_method VARCHAR(50),

    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);