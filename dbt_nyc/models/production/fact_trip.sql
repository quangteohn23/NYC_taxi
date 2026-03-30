{{ 
    config(
        materialized = 'incremental',
        unique_key = 'trip_id'
    ) 
}}

with trip_data as (
    select 
        {{ dbt_utils.surrogate_key(['vendor_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'pickup_datetime']) }} as trip_id,
        *
    from {{ ref('stg_nyc_taxi') }}
    where 1=1
    
    -- Lọc theo biến để nạp từng tháng
    {% if var('start_date', none) and var('end_date', none) %}
        and pickup_datetime >= '{{ var("start_date") }}' 
        and pickup_datetime < '{{ var("end_date") }}'
    {% endif %}

    -- Logic nạp thêm dữ liệu tự động cho sau này
    {% if is_incremental() %}
        and pickup_datetime > (select max(pickup_datetime) from {{ this }})
    {% endif %}
)

select 
    t.trip_id,
    dv.vendor_key,
    dr.rate_code_key,
    dp.payment_type_key,
    t.service_type as service_type_id,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.total_amount
from trip_data t
left join {{ ref('dim_vendor') }} dv on t.vendor_id = dv.vendor_id
left join {{ ref('dim_rate_code') }} dr on t.rate_code_id = dr.rate_code_id
left join {{ ref('dim_payment') }} dp on t.payment_type_id = dp.payment_type_id