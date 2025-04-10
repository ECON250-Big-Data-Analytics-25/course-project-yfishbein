{{ config(
    materialized='table',
    partition_by={
        "field": "order_purchase_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["order_status", "product_category_name_english"]
) }}

WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_fp_orders') }}
),

stg_order_items AS (
    SELECT * FROM {{ ref('stg_fp_order_items') }}
),

stg_products AS (
    SELECT * FROM {{ ref('stg_fp_products') }}
),

stg_customers AS (
    SELECT * FROM {{ ref('stg_fp_customers') }}
),

stg_sellers AS (
    SELECT * FROM {{ ref('stg_fp_sellers') }}
),

stg_payments AS (
    SELECT * FROM {{ ref('stg_fp_order_payments') }}
),

stg_reviews AS (
    SELECT * FROM {{ ref('stg_fp_order_reviews') }}
),

stg_category_translation AS (
    SELECT * FROM {{ ref('stg_fp_product_category_translation') }}
),

-- Aggregate payment information at order level
payment_aggregated AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_amount,
        COUNT(DISTINCT payment_type) AS payment_method_count,
        ARRAY_AGG(
            STRUCT(
                payment_sequential,
                payment_type,
                payment_installments,
                payment_value,
                is_installment_payment,
                installment_amount
            )
        ) AS payment_details,
        MAX(is_credit_card) AS used_credit_card,
        MAX(is_boleto) AS used_boleto,
        MAX(is_voucher) AS used_voucher,
        MAX(is_debit_card) AS used_debit_card,
        SUM(payment_installments) AS total_installments,
        MAX(payment_installments) AS max_installments
    FROM stg_payments
    GROUP BY order_id
),

-- Aggregate order items information at order level
order_items_aggregated AS (
    SELECT
        oi.order_id,
        COUNT(DISTINCT oi.order_item_id) AS item_count,
        SUM(oi.price) AS total_price,
        SUM(oi.freight_value) AS total_freight,
        SUM(oi.total_item_value) AS order_total_amount,
        ARRAY_AGG(
            STRUCT(
                oi.order_item_id,
                oi.product_id,
                p.product_category_name,
                ct.product_category_name_english,
                oi.price,
                oi.freight_value,
                p.product_weight_grams,
                p.product_length_cm,
                p.product_height_cm, 
                p.product_width_cm,
                p.product_photos_quantity,
                p.product_volume_cm3,
                p.has_description,
                p.has_photos,
                p.product_size_category,
                p.product_weight_category
            )
        ) AS products,
        MAX(p.product_weight_grams) AS max_product_weight_g,
        SUM(p.product_weight_grams) AS total_order_weight_g,
        COUNT(DISTINCT p.product_category_name) AS unique_product_categories,
        MAX(oi.price) AS most_expensive_item_price,
        ARRAY_AGG(STRUCT(p.product_category_name, ct.product_category_name_english, oi.price) ORDER BY oi.price DESC LIMIT 1)[OFFSET(0)].product_category_name AS main_product_category,
        ARRAY_AGG(STRUCT(p.product_category_name, ct.product_category_name_english, oi.price) ORDER BY oi.price DESC LIMIT 1)[OFFSET(0)].product_category_name_english AS main_product_category_english,
        COUNT(DISTINCT oi.seller_id) AS seller_count,
        ARRAY_AGG(
            DISTINCT STRUCT(
                oi.seller_id,
                s.seller_city,
                s.seller_state
            )
        ) AS sellers_info
    FROM stg_order_items oi
    LEFT JOIN stg_products p ON oi.product_id = p.product_id
    LEFT JOIN stg_category_translation ct ON p.product_category_name = ct.product_category_name
    LEFT JOIN stg_sellers s ON oi.seller_id = s.seller_id
    GROUP BY oi.order_id
),

review_aggregated AS (
    SELECT
        order_id,
        MAX(review_score) AS review_score,
        MAX(review_creation_date) AS review_date,
        MAX(review_answer_timestamp) AS review_answer_timestamp,
        MAX(days_to_answer) AS days_to_answer_review,
        ARRAY_AGG(
            STRUCT(
                review_id,
                review_score,
                review_comment_title,
                review_comment_message,
                review_sentiment,
                has_review_title,
                has_review_message
            )
        ) AS review_details,
        MAX(has_review_message) AS has_review_comment
    FROM stg_reviews
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    DATE(o.order_purchase_timestamp) AS order_purchase_date,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.is_delivered,
    o.is_canceled,
    o.is_shipped,
    o.is_processing,
    o.is_unavailable,
    o.is_created,
    o.is_approved,
    o.is_invoiced,
    
    -- Time-based calculations from staging
    o.days_to_delivery,
    o.days_to_approval,
    o.delivery_vs_estimated_days,
    o.delivered_on_time,
    o.is_recent_order,
    
    -- Time components for analysis
    o.order_year,
    o.order_month,
    o.order_day,
    o.order_day_of_week,
    
    -- Customer information
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    
    -- Product information from aggregation
    oia.item_count,
    oia.total_price,
    oia.total_freight,
    oia.order_total_amount,
    oia.products,
    oia.main_product_category,
    oia.main_product_category_english,
    oia.unique_product_categories,
    oia.total_order_weight_g,
    
    -- Seller information
    oia.seller_count,
    oia.sellers_info,
    
    -- Payment information from aggregation
    pa.total_payment_amount,
    pa.payment_method_count,
    pa.payment_details,
    pa.used_credit_card,
    pa.used_boleto,
    pa.used_voucher,
    pa.used_debit_card,
    pa.total_installments,
    pa.max_installments,
    
    -- Review information
    ra.review_score,
    ra.review_date,
    ra.review_answer_timestamp,
    ra.days_to_answer_review,
    ra.has_review_comment,
    ra.review_details,
    
    -- Review sentiment derived from review score
    CASE 
        WHEN ra.review_score IS NULL THEN 'no_review'
        WHEN ra.review_score >= 4 THEN 'positive'
        WHEN ra.review_score = 3 THEN 'neutral'
        ELSE 'negative'
    END AS review_sentiment,
    
    -- Time-based categorization
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(o.order_purchase_timestamp), DAY) <= 30 THEN 'last_30_days'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(o.order_purchase_timestamp), DAY) <= 90 THEN 'last_90_days'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(o.order_purchase_timestamp), DAY) <= 180 THEN 'last_180_days'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(o.order_purchase_timestamp), DAY) <= 365 THEN 'last_year'
        ELSE 'older'
    END AS order_recency,
    
    -- Current timestamp for auditing
    CURRENT_TIMESTAMP() AS model_created_at

FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items_aggregated oia ON o.order_id = oia.order_id
LEFT JOIN payment_aggregated pa ON o.order_id = pa.order_id
LEFT JOIN review_aggregated ra ON o.order_id = ra.order_id