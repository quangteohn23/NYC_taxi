{{ config(materialized='ephemeral') }}

select
    vendor_id,
    rate_code_id,
    pickup_location_id as pickup_location_id,
    dropoff_location_id as dropoff_location_id,
    payment_type_id as payment_type_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    service_type
from {{ source('postgres_source', 'nyc_taxi') }}
where vendor_id is not null