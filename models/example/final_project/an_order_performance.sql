{{ config(materialized='view') }}

SELECT
  date(order_purchase_date) date,
  count(distinct order_id) as total_orders,
  count(distinct IF(order_status = 'delivered', order_id, NULL)) as delivered_orders,
  count(distinct IF(is_canceled, order_id, NULL)) as canceled_orders,
  count(distinct IF(has_positive_review is TRUE, order_id, NULL)) as  positive_review_orders,
  count(distinct IF(is_delivered_on_time is TRUE, order_id, NULL)) as  delivered_on_time_orders,
  SUM(IF(is_canceled, 0, payment_value)) as total_revenue,
  SAFE_DIVIDE(SUM(IF(is_canceled, 0, payment_value)), count(distinct IF(is_canceled, NULL, order_id))) as AOV
FROM {{ref('fp_sales_full')}}
group by 1
order by 1 
