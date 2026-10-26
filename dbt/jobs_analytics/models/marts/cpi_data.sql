{{ config(materialized='table') }}

with src as (
    select country_code, period, value
    from {{ source('business', 'cpi_data') }}
)

select
    c.country_code,
    c.period,
    c.value                                   as cpi_index,
    c.value / nullif(py.value, 0) - 1         as cpi_yoy,
    c.value / nullif(pm.value, 0) - 1         as cpi_mom
from src c
left join src py                                   -- same month last year
    on py.country_code = c.country_code
   and py.period = c.period - interval '12 months'
left join src pm                                   -- previous month
    on pm.country_code = c.country_code
   and pm.period = c.period - interval '1 month'
