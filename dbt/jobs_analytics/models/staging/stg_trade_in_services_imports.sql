with source as (
    select * from {{ source('business', 'trade_in_services_imports') }}
),

mapped as (
    select
        cm.iso3_code as country_code,
        cm.country_name,
        extract(year from s.period)::int as year,
        extract(quarter from s.period)::int as quarter,
        s.value
    from source s
    inner join {{ source('business', 'country_mapping') }} cm
        on s."COUNTRY" = cm.iso3_code
    where s.value is not null
),

-- source values are quarterly figures forward-filled across each month in
-- the quarter, so collapse to one value per quarter before summing to a year
quarterly as (
    select
        country_code,
        country_name,
        year,
        quarter,
        avg(value) as quarterly_value
    from mapped
    group by 1, 2, 3, 4
)

select
    country_code,
    country_name,
    year,
    sum(quarterly_value) as services_imports_usd
from quarterly
group by 1, 2, 3
