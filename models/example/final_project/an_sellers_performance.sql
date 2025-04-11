{{ config(materialized='view') }}

SELECT
  oi.seller_id,
  sum(oi.product_subtotal) as seller_reveue,
  COUNT(DISTINCT order_id) seller_orders,
  COUNT(distinct customer_id) seller_customers,
  sum(oi.product_shipping_subtotal) seller_shipping_amount,
  sum(oi.order_product_qty) as seller_sold_qty,
  count(distinct IF(has_positive_review is TRUE, order_id, NULL)) as  positive_review_orders,
  count(distinct IF(is_delivered_on_time is TRUE, order_id, NULL)) as  delivered_on_time_orders
FROM {{ref('fp_sales_full')}} LEFT JOIN UNNEST(order_items) as oi
where is_canceled is FALSE and oi.seller_id is not null
group by all
order by seller_sold_qty desc