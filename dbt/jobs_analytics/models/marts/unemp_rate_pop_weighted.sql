{{ config(materialized='table') }}

WITH base_unemp AS (
	SELECT
		"weo-country" AS country_code,
		period,
		value AS unemp_value
	FROM {{ source('business', 'unemployment_data') }}
	WHERE value IS NOT NULL
),
base_pop AS (
	SELECT
		"COUNTRY" AS country_code,
		period,
		value AS pop_value
		
	FROM {{ source('business', 'population_data') }}
)
SELECT
	COALESCE(u.country_code, p.country_code) AS country_code,
	COALESCE(u.period, p.period) AS period,
	u.unemp_value,
	p.pop_value
FROM base_unemp AS u
FULL JOIN base_pop AS p 
	ON u.country_code = p.country_code
	AND u.period = p.period
	