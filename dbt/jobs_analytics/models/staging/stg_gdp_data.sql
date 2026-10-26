with source as (
    select * from {{ source('business', 'gdp_data') }}
),

mapped as (
    select
        cm.iso3_code as country_code,
        cm.country_name,
        extract(year from s.period)::int as year,
        s.value as gdp_local_currency,
        s.value_usd as gdp_usd
    from source s
    inner join {{ source('business', 'country_mapping') }} cm
        on s."REF_AREA" = cm.iso2_code
)

select
    country_code,
    country_name,
    year,
    avg(gdp_local_currency) as gdp_local_currency,
    avg(gdp_usd) as gdp_usd
from mapped
group by 1, 2, 3
