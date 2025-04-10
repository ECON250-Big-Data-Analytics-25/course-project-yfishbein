{{ config(materialized='view') }}

select
    product_id,
    coalesce(product_category_name, '(not set)') as product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_length_cm * product_height_cm * product_width_cm  as product_volume_cm3,
    case 
        when product_description_lenght > 0 then true
        else false
    end as has_description,
    case 
        when product_photos_qty > 0 then true
        else false
    end as has_photos,
    case
        when product_length_cm * product_height_cm * product_width_cm is null then '(not set)'
        when product_length_cm * product_height_cm * product_width_cm > 20000 then 'large'
        when product_length_cm * product_height_cm * product_width_cm > 10000 then 'medium'
        else 'small'
    end as product_size_category,
    case    
        when product_weight_g is null then '(not set)'
        when product_weight_g > 5000 then 'heavy'
        when product_weight_g > 2500 then 'medium'
        else 'light'
    end as product_weight_category
from {{ source('yfishbein', 'fp_olist_products_dataset') }}