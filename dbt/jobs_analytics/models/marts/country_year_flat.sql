{{ config(materialized='table') }}

with countries as (
    select
        iso3_code as country_code,
        country_name,
        continent
    from {{ source('business', 'country_mapping') }}
),

years as (
    select year from {{ ref('stg_cpi_data') }}
    union select year from {{ ref('stg_gdp_data') }}
    union select year from {{ ref('stg_exchange_rate') }}
    union select year from {{ ref('stg_cb_policy_rate_data') }}
    union select year from {{ ref('stg_ipi_data') }}
    union select year from {{ ref('stg_unemployment_data') }}
    union select year from {{ ref('stg_retail_sales_data') }}
    union select year from {{ ref('stg_global_manufacturing_pmi_data') }}
    union select year from {{ ref('stg_population_data') }}
    union select year from {{ ref('stg_merchandise_exports_data') }}
    union select year from {{ ref('stg_merchandise_imports_data') }}
    union select year from {{ ref('stg_trade_in_services_exports') }}
    union select year from {{ ref('stg_trade_in_services_imports') }}
),

spine as (
    select
        c.country_code,
        c.country_name,
        c.continent,
        y.year
    from countries c
    cross join years y
),

flat as (
    select
        spine.country_code,
        spine.country_name,
        spine.continent,
        spine.year,
        cpi.cpi,
        gdp.gdp_local_currency,
        gdp.gdp_usd,
        fx.exchange_rate_usd,
        policy.policy_rate,
        ipi.industrial_production_index,
        unemp.unemployment_rate,
        retail.retail_sales_index,
        pmi.manufacturing_pmi,
        pop.population,
        exports.merchandise_exports_usd,
        imports.merchandise_imports_usd,
        svc_exports.services_exports_usd,
        svc_imports.services_imports_usd
    from spine
    left join {{ ref('stg_cpi_data') }} cpi
        on spine.country_code = cpi.country_code and spine.year = cpi.year
    left join {{ ref('stg_gdp_data') }} gdp
        on spine.country_code = gdp.country_code and spine.year = gdp.year
    left join {{ ref('stg_exchange_rate') }} fx
        on spine.country_code = fx.country_code and spine.year = fx.year
    left join {{ ref('stg_cb_policy_rate_data') }} policy
        on spine.country_code = policy.country_code and spine.year = policy.year
    left join {{ ref('stg_ipi_data') }} ipi
        on spine.country_code = ipi.country_code and spine.year = ipi.year
    left join {{ ref('stg_unemployment_data') }} unemp
        on spine.country_code = unemp.country_code and spine.year = unemp.year
    left join {{ ref('stg_retail_sales_data') }} retail
        on spine.country_code = retail.country_code and spine.year = retail.year
    left join {{ ref('stg_global_manufacturing_pmi_data') }} pmi
        on spine.country_code = pmi.country_code and spine.year = pmi.year
    left join {{ ref('stg_population_data') }} pop
        on spine.country_code = pop.country_code and spine.year = pop.year
    left join {{ ref('stg_merchandise_exports_data') }} exports
        on spine.country_code = exports.country_code and spine.year = exports.year
    left join {{ ref('stg_merchandise_imports_data') }} imports
        on spine.country_code = imports.country_code and spine.year = imports.year
    left join {{ ref('stg_trade_in_services_exports') }} svc_exports
        on spine.country_code = svc_exports.country_code and spine.year = svc_exports.year
    left join {{ ref('stg_trade_in_services_imports') }} svc_imports
        on spine.country_code = svc_imports.country_code and spine.year = svc_imports.year
)

select *
from flat
where coalesce(cpi, gdp_local_currency, gdp_usd, exchange_rate_usd, policy_rate,
                industrial_production_index, unemployment_rate, retail_sales_index,
                manufacturing_pmi, population, merchandise_exports_usd,
                merchandise_imports_usd, services_exports_usd, services_imports_usd) is not null
