with source as (
    select * from {{ source('business', 'merchandise_imports_data') }}
),

mapped as (
    select
        cm.iso3_code as country_code,
        cm.country_name,
        substring(s.period, 1, 4)::int as year,
        s.value
    from source s
    inner join {{ source('business', 'country_mapping') }} cm
        on s."COUNTRY" = cm.iso3_code
    where s.value is not null
)

select
    country_code,
    country_name,
    year,
    sum(value) as merchandise_imports_usd
from mapped
group by 1, 2, 3
