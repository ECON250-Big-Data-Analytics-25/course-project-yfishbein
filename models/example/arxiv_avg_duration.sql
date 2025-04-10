{{ config(materialized='view') }}

 select AVG(DATE_DIFF(updated_date, published_date, DAY)) as average_duration
 from {{source('test_dataset', 'arxiv')}}
