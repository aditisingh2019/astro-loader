/*
Purpose:
--------
Define CORE (3NF) database schema.

Responsibilities:
-----------------
- Store trusted, relational data.
- Serve analytics and downstream systems.
- Enforce strong data integrity.

Design Notes:
-------------
- No stg_ prefix.
- Fully normalized (3NF).
- Populated only via SQL transforms.
*/


CREATE TABLE IF NOT EXISTS booking_statuses (
    status_id   SERIAL PRIMARY KEY,
    status_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS vehicle_types (
    vehicle_type_id   SERIAL PRIMARY KEY,
    vehicle_type_name VARCHAR(30) UNIQUE
);

CREATE TABLE IF NOT EXISTS locations (
    location_id   SERIAL PRIMARY KEY,
    location_name VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    method_name       VARCHAR(20) UNIQUE
);

CREATE TABLE IF NOT EXISTS cancellation_reasons (
    reason_id          SERIAL PRIMARY KEY,
    reason_description VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS incomplete_reasons (
    reason_id          SERIAL PRIMARY KEY,
    reason_description VARCHAR(100) UNIQUE
);


CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(20) PRIMARY KEY,
    customer_rating DECIMAL(3,2)
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id     INTEGER PRIMARY KEY,
    driver_rating DECIMAL(3,2)
);


CREATE TABLE IF NOT EXISTS bookings (
    booking_id VARCHAR(20) PRIMARY KEY,
    booking_date DATE,
    booking_time TIME,

    status_id INTEGER REFERENCES booking_statuses(status_id),
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    driver_id INTEGER REFERENCES drivers(driver_id),
    vehicle_type_id INTEGER REFERENCES vehicle_types(vehicle_type_id),

    pickup_location_id INTEGER REFERENCES locations(location_id),
    drop_location_id INTEGER REFERENCES locations(location_id),

    avg_vtat DECIMAL(6,2),
    avg_ctat DECIMAL(6,2),
    booking_value DECIMAL(10,2),
    ride_distance DECIMAL(6,2),

    customer_rating DECIMAL(3,2),
    payment_method_id INTEGER REFERENCES payment_methods(payment_method_id)
);


CREATE TABLE IF NOT EXISTS customer_cancellations (
    booking_id VARCHAR(20) PRIMARY KEY REFERENCES bookings(booking_id),
    reason_id INTEGER REFERENCES cancellation_reasons(reason_id)
);

CREATE TABLE IF NOT EXISTS driver_cancellations (
    booking_id VARCHAR(20) PRIMARY KEY REFERENCES bookings(booking_id),
    reason_id INTEGER REFERENCES cancellation_reasons(reason_id)
);

CREATE TABLE IF NOT EXISTS incomplete_rides (
    booking_id VARCHAR(20) PRIMARY KEY REFERENCES bookings(booking_id),
    reason_id INTEGER REFERENCES incomplete_reasons(reason_id)
);
