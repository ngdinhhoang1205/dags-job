{{ config(materialized='table') }}

WITH base_policy AS (
	SELECT
		cm.iso3_code AS country_code,
		s.period,
		EXTRACT(YEAR FROM s.period)::int AS year,
		s.value AS policy_rate
	FROM {{ source('business', 'cb_policy_rate_data') }} s
	INNER JOIN {{ source('business', 'country_mapping') }} cm
		ON s."REF_AREA" = cm.iso2_code
	WHERE s.value IS NOT NULL
),
base_gdp AS (
	SELECT
		cm.iso3_code AS country_code,
		EXTRACT(YEAR FROM s.period)::int AS year,
		AVG(s.value_usd) AS gdp_usd
	FROM {{ source('business', 'gdp_data') }} s
	INNER JOIN {{ source('business', 'country_mapping') }} cm
		ON s."REF_AREA" = cm.iso2_code
	WHERE s.value_usd IS NOT NULL
	GROUP BY 1, 2
)
SELECT
	COALESCE(p.country_code, g.country_code) AS country_code,
	COALESCE(p.year, g.year) AS year,
	p.period,
	p.policy_rate,
	g.gdp_usd
FROM base_policy AS p
FULL JOIN base_gdp AS g
	ON p.country_code = g.country_code
	AND p.year = g.year
