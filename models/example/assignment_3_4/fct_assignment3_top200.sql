{{ config(materialized='table') }}

with

prep as (
    select 
    title,
    sum(views) as total_views,
    sum(IF(src = 'mobile', views, 0)) as total_mobile_views,
    FORMAT('%.2f%%',
        SAFE_DIVIDE(
            sum(IF(src = 'mobile', views, 0)), 
            sum(views)
        ) * 100
    ) as mobile_percentage,
    from {{ref('int_assignment3_uk_wiki')}}
    where is_meta_page is FALSE
    group by title
    order by total_views desc
    limit 200
)

select * from prep
order by mobile_percentage asc
