{{ config(materialized='view') }}

 select AVG(DATE_DIFF(update_date, published_date)) as average_duration
 from {{source('test_dataset', 'arxiv')}}
