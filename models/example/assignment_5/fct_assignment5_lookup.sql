{{
  config(
    materialized = 'incremental',
    unique_key = 'title'
  )
}}

WITH source_data AS (
  SELECT 
    date(datehour) as date,
    title,
    views
  FROM {{ source('test_dataset', 'assignment5_input') }}
  
  {% if is_incremental() %}
    WHERE date(datehour) >= (SELECT max(date(datehour)) FROM {{ this }}) - 1
  {% endif %}
),

aggregated_data AS (
  SELECT
    title,
    MIN(date) as min_date,
    MAX(date) as max_date,
    SUM(views) as total_views
  FROM source_data
  GROUP BY 1
)

SELECT
  title,
  min_date,
  max_date,
  total_views
FROM aggregated_data