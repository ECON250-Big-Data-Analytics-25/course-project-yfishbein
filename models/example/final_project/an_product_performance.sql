{{ config(materialized='view') }}

SELECT
  oi.product_id,
  oi.category_name,
  oi.category_name_english,
  oi.size_category,
  oi.weight_category,
  sum(oi.product_subtotal) as product_reveue,
  COUNT(DISTINCT order_id) orders_with_product,
  COUNT(distinct customer_id) customers_with_product,
  sum(oi.product_shipping_subtotal) product_shipping_amount,
  sum(oi.order_product_qty) as sold_qty
FROM {{ref('fp_sales_full')}} LEFT JOIN UNNEST(order_items) as oi
where is_canceled is FALSE and oi.product_id is not null
group by all
order by orders_with_product desc