{{
  config(
    materialized = 'incremental',
    incremental_strategy = "insert_overwrite",
    partition_by = {
      "field": "date",
      "data_type": "date"
    }
  )
}}

WITH source_data AS (
  SELECT 
    date(datehour) as date,
    title,
    views
  FROM {{ source('test_dataset', 'assignment5_input') }}
  
  {% if is_incremental() %}
    WHERE date(datehour) >= {{ _dbt_max_partition - 1 }}
  {% endif %}
)

SELECT
  date,
  title,
  SUM(views) as views
FROM source_data
GROUP BY 1, 2