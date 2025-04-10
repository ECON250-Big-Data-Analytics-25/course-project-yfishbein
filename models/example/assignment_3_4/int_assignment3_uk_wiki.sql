{{ config(materialized='view') }}

select 
*,
{{wiki_is_meta_page('title')}} as is_meta_page,
IF(
    {{wiki_is_meta_page('title')}},
    SPLIT(title, ':')[OFFSET(0)],
    CAST (NULL AS STRING)
) as meta_page_type
from {{ref('stg_assignment3_uk_wiki')}}