/*
Purpose:
--------
Transform and load data from STAGING (stg_rides) into CORE schema tables.

Behavior:
---------
1. Populate dimension tables
2. Populate entity tables (customers, drivers)
3. Populate bookings fact table
4. Populate cancellation / incomplete tables
*/

CREATE OR REPLACE PROCEDURE transfer_stage_to_core()
LANGUAGE plpgsql
AS $$
BEGIN


-- Booking Status
INSERT INTO booking_statuses (status_name)
SELECT DISTINCT booking_status
FROM stg_rides
WHERE booking_status IS NOT NULL
ON CONFLICT (status_name) DO NOTHING;


-- Vehicle Types
INSERT INTO vehicle_types (vehicle_type_name)
SELECT DISTINCT vehicle_type
FROM stg_rides
WHERE vehicle_type IS NOT NULL
ON CONFLICT (vehicle_type_name) DO NOTHING;


-- Locations (pickup & drop)
INSERT INTO locations (location_name)
SELECT DISTINCT pickup_location
FROM stg_rides
WHERE pickup_location IS NOT NULL
ON CONFLICT (location_name) DO NOTHING;

INSERT INTO locations (location_name)
SELECT DISTINCT drop_location
FROM stg_rides
WHERE drop_location IS NOT NULL
ON CONFLICT (location_name) DO NOTHING;


-- Payment Methods
INSERT INTO payment_methods (method_name)
SELECT DISTINCT payment_method
FROM stg_rides
WHERE payment_method IS NOT NULL
ON CONFLICT (method_name) DO NOTHING;


-- Cancellation Reasons (customer & driver)
INSERT INTO cancellation_reasons (reason_description)
SELECT DISTINCT reason_for_cancelling_by_customer
FROM stg_rides
WHERE reason_for_cancelling_by_customer IS NOT NULL

UNION

SELECT DISTINCT driver_cancellation_reason
FROM stg_rides
WHERE driver_cancellation_reason IS NOT NULL
ON CONFLICT (reason_description) DO NOTHING;


-- Incomplete Reasons
INSERT INTO incomplete_reasons (reason_description)
SELECT DISTINCT incomplete_rides_reason
FROM stg_rides
WHERE incomplete_rides_reason IS NOT NULL
ON CONFLICT (reason_description) DO NOTHING;


-- Customers 
INSERT INTO customers (customer_id)
SELECT DISTINCT customer_id
FROM stg_rides
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_id) DO NOTHING;


INSERT INTO bookings (
    booking_id,
    booking_date,
    booking_time,
    status_id,
    customer_id,
    vehicle_type_id,
    pickup_location_id,
    drop_location_id,
    avg_vtat,
    avg_ctat,
    booking_value,
    ride_distance,
    driver_rating,
    customer_rating,
    payment_method_id
)
SELECT
    s.booking_id,
    s.booking_date,
    s.booking_time,
    bs.status_id,
    s.customer_id,
    vt.vehicle_type_id,
    lp.location_id,
    ld.location_id,
    s.avg_vtat,
    s.avg_ctat,
    s.booking_value,
    s.ride_distance,
    s.driver_ratings,
    s.customer_rating,
    pm.payment_method_id
FROM stg_rides s
LEFT JOIN booking_statuses bs
    ON s.booking_status = bs.status_name
LEFT JOIN vehicle_types vt
    ON s.vehicle_type = vt.vehicle_type_name
LEFT JOIN locations lp
    ON s.pickup_location = lp.location_name
LEFT JOIN locations ld
    ON s.drop_location = ld.location_name
LEFT JOIN payment_methods pm
    ON s.payment_method = pm.method_name
WHERE s.booking_id IS NOT NULL
ON CONFLICT (booking_id) DO NOTHING;


-- Customer cancellations
INSERT INTO cancellations (booking_id, cancelled_by, reason_id)
SELECT
    s.booking_id,
    'CUSTOMER',
    cr.reason_id
FROM stg_rides s
JOIN cancellation_reasons cr
    ON s.reason_for_cancelling_by_customer = cr.reason_description
WHERE s.cancelled_rides_by_customer = 1
ON CONFLICT (booking_id) DO NOTHING;


-- Driver cancellations
INSERT INTO cancellations (booking_id, cancelled_by, reason_id)
SELECT
    s.booking_id,
    'DRIVER',
    cr.reason_id
FROM stg_rides s
JOIN cancellation_reasons cr
    ON s.driver_cancellation_reason = cr.reason_description
WHERE s.cancelled_rides_by_driver = 1
ON CONFLICT (booking_id) DO NOTHING;


INSERT INTO incomplete_rides (booking_id, reason_id)
SELECT
    s.booking_id,
    ir.reason_id
FROM stg_rides s
JOIN incomplete_reasons ir
    ON s.incomplete_rides_reason = ir.reason_description
WHERE s.incomplete_rides = 1
ON CONFLICT (booking_id) DO NOTHING;


END;
$$;
