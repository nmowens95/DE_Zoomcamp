{{ config(materialized='table') }}

select 
    LocationID as locationid, 
    Borough as borough, 
    Zone as zone, 
    replace(service_zone,'Boro','Green') as service_zone 
from {{ source('staging','zone_lookup') }}