{{ config(materialized='view') }}

select
    coalesce(product_category_name, '(not set)') as product_category_name,
    coalesce(product_category_name_english, '(not set)') as product_category_name_english,
    case
        when product_category_name_english is null then '(not set)'
        when product_category_name_english like '%health%' then 'health'
        when product_category_name_english like '%beauty%' then 'beauty'
        when product_category_name_english like '%sports%' then 'sports'
        when product_category_name_english like '%furniture%' then 'furniture'
        when product_category_name_english like '%house%' then 'home'
        when product_category_name_english like '%home%' then 'home'
        when product_category_name_english like '%garden%' then 'home'
        when product_category_name_english like '%kitchen%' then 'home'
        when product_category_name_english like '%bath%' then 'home'
        when product_category_name_english like '%bed%' then 'home'
        when product_category_name_english like '%computer%' then 'electronics'
        when product_category_name_english like '%electronic%' then 'electronics'
        when product_category_name_english like '%phone%' then 'electronics'
        when product_category_name_english like '%watch%' then 'fashion'
        when product_category_name_english like '%clothing%' then 'fashion'
        when product_category_name_english like '%fashion%' then 'fashion'
        when product_category_name_english like '%toy%' then 'toys'
        when product_category_name_english like '%baby%' then 'baby'
        when product_category_name_english like '%book%' then 'books'
        when product_category_name_english like '%office%' then 'office'
        when product_category_name_english like '%auto%' then 'automotive'
        when product_category_name_english like '%tool%' then 'tools'
        when product_category_name_english like '%market%' then 'grocery'
        when product_category_name_english like '%food%' then 'grocery'
        when product_category_name_english like '%drink%' then 'grocery'
        when product_category_name_english like '%pet%' then 'pet'
        when product_category_name_english like '%construction%' then 'construction'
        when product_category_name_english like '%luggage%' then 'travel'
        when product_category_name_english like '%art%' then 'art'
        when product_category_name_english like '%music%' then 'entertainment'
        when product_category_name_english like '%game%' then 'entertainment'
        when product_category_name_english like '%party%' then 'entertainment'
        else 'other'
    end as category_group
from {{ source('yfishbein', 'fp_product_category_name_translation') }}