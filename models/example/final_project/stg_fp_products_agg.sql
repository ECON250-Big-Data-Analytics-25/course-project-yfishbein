{{ config(materialized='view') }}

SELECT 
    product_id,
    product_category_name as category_name,
    product_category_name_english as category_name_english,
    category_group,
    product_name_lenght as name_lenght,
    product_description_lenght as description_lenght,
    product_photos_qty as photos_qty,
    has_description,
    has_photos,
    product_weight_g as weight_g,
    product_length_cm as length_cm,
    product_width_cm as width_cm,
    product_height_cm as height_cm,
    product_volume_cm3 as volume_cm3,
    product_size_category as size_category,
    product_weight_category as weight_category
FROM {{ ref('stg_fp_products') }}
LEFT JOIN {{ ref ('stg_fp_product_category') }} USING(product_category_name)

