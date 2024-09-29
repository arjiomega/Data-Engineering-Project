{{ 
    config(
        materialized='table'
    ) 
}}
select 
    tzl.locationid as location_id, 
    tzl.borough, 
    tzl.Zone as zone, 
    REPLACE(tzl.service_zone,'Boro Zone','Green Zone') as service_zone 
from {{ ref('taxi_zone_lookup') }} tzl