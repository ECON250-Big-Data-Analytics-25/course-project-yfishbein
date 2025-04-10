{{ config(materialized='view') }}

with least_mobile_from_table as (
    select 
        title, 
        mobile_percentage, 
    from {{ref('fct_assignment3_top200')}}
    order by mobile_percentage asc
    limit 1
)


SELECT 
hour_of_day,
sum(views) as total_views,
sum(IF(src = 'mobile', views, 0)) as total_mobile_views,
FROM {{ref('int_assignment3_uk_wiki')}} where title in (select title from least_mobile_from_table)
group by 1 
order by 1