{{ config(materialized = 'view') }}
with base as (
    select distinct vendor_id from {{ ref('stg_nyc_taxi') }}
)
select 
    {{ dbt_utils.surrogate_key(['vendor_id']) }} as vendor_key,
    vendor_id,
    {{ get_vendor_description('vendor_id') }} as vendor_name
from base
where vendor_id is not null