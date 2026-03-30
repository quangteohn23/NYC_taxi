{{ config(materialized = 'view') }}
with all_locs as (
    select pickup_location_id as location_id from {{ ref('stg_nyc_taxi') }}
    union
    select dropoff_location_id as location_id from {{ ref('stg_nyc_taxi') }}
)
select * from all_locs where location_id is not null