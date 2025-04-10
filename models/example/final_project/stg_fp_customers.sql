{{ config(materialized='view') }}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    upper(
        coalesce(customer_city, '(not set)')
    ) as customer_city,
    upper(
        coalesce(customer_state, '(not set)')
    ) as customer_state,
from {{ source('yfishbein', 'fp_olist_customers_dataset') }}
