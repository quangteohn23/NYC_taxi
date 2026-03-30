{{ config(materialized = 'view') }}
with base as (
    select distinct rate_code_id from {{ ref('stg_nyc_taxi') }}
)
select 
    {{ dbt_utils.surrogate_key(['rate_code_id']) }} as rate_code_key,
    rate_code_id,
    {{ get_rate_code_description('rate_code_id') }} as rate_code_description
from base
where rate_code_id is not null