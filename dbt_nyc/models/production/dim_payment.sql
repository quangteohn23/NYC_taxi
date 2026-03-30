{{ config(materialized = 'view') }}
with base as (
    select distinct payment_type_id from {{ ref('stg_nyc_taxi') }}
)
select 
    {{ dbt_utils.surrogate_key(['payment_type_id']) }} as payment_type_key,
    payment_type_id,
    {{ get_payment_description('payment_type_id') }} as payment_description
from base
where payment_type_id is not null