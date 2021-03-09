SELECT
license,
count(1) as n_of_licenses
FROM {{ source('Github', 'licenses')}}
GROUP BY license
ORDER BY n_of_licenses DESC