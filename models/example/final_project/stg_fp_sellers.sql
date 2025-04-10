{{ config(materialized='view') }}

select
    seller_id,
    seller_zip_code_prefix as seller_zip_code_prefix,
    upper(
        coalesce(seller_city, '(not set)')
    ) as seller_city,
    upper(
        coalesce(seller_state, '(not set)')
    ) as seller_state
from {{ source('yfishbein', 'fp_olist_sellers_dataset') }}