{{ config(materialized='view') }}

with
cte_combined as (
    select 
    *,
    'desktop' as src
    from {{source('test_dataset','assignment3_input_uk')}}

    UNION ALL 

    select 
    *,
    'mobile' as src
    from {{source('test_dataset','assignment3_input_uk_m')}}
)

select 
*, 
date(datehour) as date,
EXTRACT(DAYOFWEEK FROM datehour) as day_of_week,
EXTRACT(HOUR FROM datehour) as hour_of_day
from cte_combined