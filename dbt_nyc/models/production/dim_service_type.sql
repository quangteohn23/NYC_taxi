{{ config(materialized = 'view') }}
with base as (
    select distinct service_type from {{ ref('stg_nyc_taxi') }}
)
select 
    service_type as service_type_id,
    {{ get_service_name('service_type') }} as service_name
from base
where service_type is not null