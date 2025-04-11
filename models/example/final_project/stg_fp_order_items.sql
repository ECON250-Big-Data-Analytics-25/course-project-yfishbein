{{ config(materialized='view') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,    
    price,
    freight_value,
    IFNULL(price,0) + IFNULL(freight_value,0) as total_order_value,
    case 
        when price = 0 then true
        else false
    end as is_free_item,
    case 
        when freight_value = 0 then true
        else false
    end as is_free_shipping
from  {{ source('yfishbein', 'fp_olist_order_items_dataset') }}
