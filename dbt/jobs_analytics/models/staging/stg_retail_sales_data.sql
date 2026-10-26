with source as (
    select * from {{ source('business', 'retail_sales_data') }}
),

mapped as (
    select
        cm.iso3_code as country_code,
        cm.country_name,
        extract(year from s.period)::int as year,
        s.value
    from source s
    inner join {{ source('business', 'country_mapping') }} cm
        on s."LOCATION" = cm.iso3_code
    where s.value is not null
)

select
    country_code,
    country_name,
    year,
    avg(value) as retail_sales_index
from mapped
group by 1, 2, 3
