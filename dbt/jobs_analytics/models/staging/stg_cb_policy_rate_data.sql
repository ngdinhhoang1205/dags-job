with source as (
    select * from {{ source('business', 'cb_policy_rate_data') }}
),

mapped as (
    select
        cm.iso3_code as country_code,
        cm.country_name,
        extract(year from s.period)::int as year,
        s.value
    from source s
    inner join {{ source('business', 'country_mapping') }} cm
        on s."REF_AREA" = cm.iso2_code
    where s.value is not null
)

select
    country_code,
    country_name,
    year,
    avg(value) as policy_rate
from mapped
group by 1, 2, 3
