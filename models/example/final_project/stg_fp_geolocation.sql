{{ config(materialized='view') }}

select
    geolocation_zip_code_prefix as zip_code_prefix,
    geolocation_lat as latitude,
    geolocation_lng as longitude,
    upper(
        coalesce(geolocation_city, '(not set)')
     ) as city,
    upper(
        coalesce(geolocation_state, '(not set)')
    ) as state,
    (geolocation_lat is not null and geolocation_lng is not null) as has_valid_coordinates,
from  {{ source('yfishbein', 'fp_olist_geolocation_dataset') }}

