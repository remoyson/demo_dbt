{{
    config(
        materialized='ephemeral'
    )
}}

WITH commits_with_timezone as (
SELECT 
commits.*,
mapping.timezone,
DATETIME(TIMESTAMP_SECONDS(commits.committer.time_sec), mapping.timezone) as committer_local_datetime
FROM {{ source('github', 'sample_commits')}} as commits

LEFT JOIN {{ ref('mapping_time_offset_zone')}} as mapping
ON commits.committer.tz_offset = mapping.tz_offset
)


SELECT committer_local_datetime
FROM commits_with_timezone