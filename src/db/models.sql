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

CREATE TABLE IF NOT EXISTS stg_rejects (
    reject_id           BIGSERIAL PRIMARY KEY,
    source_name         TEXT NOT NULL,
    raw_record          JSONB NOT NULL,
    reject_reason       TEXT NOT NULL,

    ingestion_ts        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_booking_status (
	status_id SERIAL PRIMARY KEY,
	status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS stg_vehicle_types (
	vehicle_type_id SERIAL PRIMARY KEY,
	vehicle_name VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS stg_locations (
	location_id SERIAL PRIMARY KEY,
	"location" VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS stg_customers (
	customer_id VARCHAR(20) PRIMARY KEY,
	customer_rating DECIMAL
);

CREATE TABLE IF NOT EXISTS stg_drivers (
	driver_id SERIAL PRIMARY KEY,
	driver_rating DECIMAL
);

CREATE TABLE IF NOT EXISTS stg_bookings (
    booking_id VARCHAR(20) PRIMARY KEY,
    booking_date DATE,
    booking_time TIME,
    booking_status_id INTEGER,
	customer_id VARCHAR(20),
    driver_id INTEGER,
    vehicle_type_id INTEGER,
    pickup_location_id INTEGER, 
    drop_location_id INTEGER,
    avg_vtat DECIMAL,
    avg_ctat DECIMAL,
    booking_value DECIMAL,
    ride_distance DECIMAL,
    payment_method VARCHAR(20),
	FOREIGN KEY (booking_status_id) REFERENCES booking_status(status_id),
	FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
	FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
	FOREIGN KEY (vehicle_type_id) REFERENCES vehicle_types(vehicle_type_id),
	FOREIGN KEY (pickup_location_id) REFERENCES locations(location_id),
	FOREIGN KEY (drop_location_id) REFERENCES locations(location_id)
);

CREATE TABLE IF NOT EXISTS stg_cancellations (
	cancellation_id SERIAL PRIMARY KEY,
	booking_id VARCHAR(20),
	cancelled_by_customer_id INTEGER,
	cancelled_by_driver_id INTEGER,
	cancellation_reason VARCHAR(50),
	FOREIGN KEY (booking_id) REFERENCES bookings(booking_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stg_incomplete_rides (
	incomplete_ride_id SERIAL PRIMARY KEY,
	booking_id VARCHAR(20),
	incomplete_ride_reason VARCHAR(50),
	FOREIGN KEY (booking_id) REFERENCES bookings(booking_id) ON DELETE CASCADE
);
