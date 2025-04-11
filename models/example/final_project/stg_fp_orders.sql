{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    coalesce(order_status, 'unknown') as order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    case when lower(order_status) = 'delivered' then true else false end as is_delivered,
    case when lower(order_status) = 'shipped' then true else false end as is_shipped,
    case when lower(order_status) = 'canceled' then true else false end as is_canceled,
    date_diff(
        date(order_delivered_customer_date),
        date(order_purchase_timestamp),
        DAY
    ) as days_to_delivery,
    date_diff(
        date(order_approved_at),
        date(order_purchase_timestamp),
        DAY
    ) as days_to_approval,
    date_diff(
        date(order_delivered_customer_date), 
        date(order_estimated_delivery_date),
        DAY
    ) as delivery_vs_estimated_days,
    case 
        when order_delivered_customer_date is not null 
            and cast(order_delivered_customer_date as date) <= cast(order_estimated_delivery_date as date) 
        then true
        else false
    end as is_delivered_on_time,
    extract(year from cast(order_purchase_timestamp as timestamp)) as order_year,
    extract(month from cast(order_purchase_timestamp as timestamp)) as order_month,
    extract(day from cast(order_purchase_timestamp as timestamp)) as order_day,
    extract(dayofweek from cast(order_purchase_timestamp as timestamp)) as order_day_of_week,
from {{ source('yfishbein', 'fp_olist_orders_dataset') }}