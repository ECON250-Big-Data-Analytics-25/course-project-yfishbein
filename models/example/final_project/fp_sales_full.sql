{{ config(
    materialized='table',

    cluster_by=["order_status"]
) }}

WITH 
stg_geo as (
    select 
    zip_code_prefix,
    struct(
        latitude,
        longitude,
        city,
        state,
        has_valid_coordinates
    ) as geo
    from {{ ref('stg_fp_geolocation') }}
),

stg_sellers AS (
    SELECT 
        seller_id,
        stg_geo.geo as seller_geo,
        seller_city,
        seller_state
    FROM {{ ref('stg_fp_sellers') }}
    left join stg_geo using (zip_code_prefix)
),

stg_customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        stg_geo.geo as customer_geo,
        customer_city,
        customer_state
    FROM {{ ref('stg_fp_customers') }}
    left join stg_geo using (zip_code_prefix)
),

stg_products AS (
    SELECT * FROM {{ ref('stg_fp_products_agg') }}
),

stg_order_items AS (
    select 
    order_id,
    ARRAY_AGG(
        STRUCT(
            product_id,
            category_name,
            category_name_english,
            category_group,
            name_lenght,
            description_lenght,
            photos_qty,
            has_description,
            has_photos,
            weight_g,
            length_cm,
            width_cm,
            height_cm,
            volume_cm3,
            size_category,
            weight_category,
            order_product_qty,
            seller_id,
            --seller_geo,
            shipping_limit_date,
            price_per_item,
            freight_value_per_item,
            product_subtotal,
            product_shipping_subtotal
        )
    ) as order_items
    from (
        SELECT DISTINCT
            order_id,
            product_id,
            category_name,
            category_name_english,
            category_group,
            name_lenght,
            description_lenght,
            photos_qty,
            has_description,
            has_photos,
            weight_g,
            length_cm,
            width_cm,
            height_cm,
            volume_cm3,
            size_category,
            weight_category,
            count(product_id) over (partition by order_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as order_product_qty,
            seller_id,
            --seller_geo,
            shipping_limit_date,
            price as price_per_item,
            freight_value as freight_value_per_item,
            sum(price) over (partition by order_id, product_id) product_subtotal,
            sum(freight_value) over (partition by order_id, product_id) product_shipping_subtotal,
        FROM {{ ref('stg_fp_order_items') }}
        left join stg_products using(product_id)
        left join stg_sellers using(seller_id)
    )
    GROUP BY order_id
),

stg_payments AS (
    SELECT
    order_id,
    payment_value,
    payment_sequential,
    struct(
        payment_type,
        payment_installments,
        is_installment_payment,
        installment_amount,
        is_credit_card,
        is_boleto,
        is_voucher,
        is_debit_card
    ) as payment_details
    FROM {{ ref('stg_fp_order_payments') }}
),

stg_reviews AS (
    SELECT 
        order_id,
        ARRAY_AGG(
            STRUCT(
                review_id,
                review_dimensions,
                review_timing
            )
        ) as order_reviews,
        MAX(review_dimensions.is_positive_review) as has_positive_review,
        MAX(review_dimensions.is_negative_review) as has_negative_review
    FROM(
        SELECT 
            review_id,
            order_id,
            struct(
                review_score,
                review_comment_title,
                review_comment_message,
                review_sentiment,
                is_positive_review,
                is_negative_review,
                has_review_title,
                has_review_message
            ) as review_dimensions,
            struct(
                review_creation_date,
                review_answer_timestamp,
                review_hour,
                review_day_week,
                days_to_answer,
                hours_to_answer
            ) as review_timing
        FROM {{ ref('stg_fp_order_reviews') }}
    )
    GROUP BY order_id
),

stg_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        date(order_purchase_timestamp) order_purchase_date,
        struct(
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            is_delivered,
            is_shipped,
            days_to_delivery,
            days_to_approval,
            delivery_vs_estimated_days,
            is_delivered_on_time
        ) as order_details,
        payment_value,
        payment_sequential,
        payment_details,
        order_items,
        order_reviews,
        has_positive_review,
        has_negative_review
        is_delivered_on_time,
        is_canceled
    FROM {{ ref('stg_fp_orders') }}
    left join stg_order_items using(order_id)
    left join stg_payments using(order_id)
    left join stg_reviews using(order_id)
)

select * from stg_orders