{{ config(materialized='view') }}

select *
from {{source('google_trends', 'international_top_terms')}} as trends
left join (select * from {{ref('all')}}) as cc on  trends.country_name = cc.name
