{{ config(materialized='view') }}

select
    review_id,
    order_id,
    cast(review_score as integer) as review_score,
    coalesce(review_comment_title, '') as review_comment_title,
    coalesce(review_comment_message, '') as review_comment_message,
    review_creation_date as review_creation_date,
    review_answer_timestamp as review_answer_timestamp,
    EXTRACT(HOUR FROM review_answer_timestamp) as review_hour,
    EXTRACT(DAYOFWEEK FROM review_answer_timestamp) as review_day_week, 
    case
        when cast(review_score as integer) >= 4 then 'positive'
        else 'negative'
    end as review_sentiment,
    review_score >=4 as is_positive_review,
    review_score < 4 as is_negative_review,
    case when length(coalesce(review_comment_title, '')) > 0 then true else false end as has_review_title,
    case when length(coalesce(review_comment_message, '')) > 0 then true else false end as has_review_message,
    date_diff(
        date(review_answer_timestamp), 
        date(review_creation_date), 
        DAY
    ) as days_to_answer,
    TIMESTAMP_DIFF(
        review_answer_timestamp, 
        review_creation_date, 
        HOUR
    ) as hours_to_answer,
from {{ source('yfishbein', 'fp_olist_order_reviews_dataset') }}