WITH commits_with_timezone as (
SELECT 
commits.*,
mapping.timezone,
DATETIME(TIMESTAMP_SECONDS(commits.committer.time_sec), mapping.timezone) as committer_local_datetime
{% if target.name == 'dev' %}
FROM {{ source('github', 'sample_commits')}} as commits
{% else %}
FROM {{ source('github', 'commits')}} as commits
{% endif %}

LEFT JOIN {{ ref('mapping_time_offset_zone')}} as mapping
ON commits.committer.tz_offset = mapping.tz_offset
)


SELECT committer_local_datetime, TIMESTAMP_SECONDS(committer.time_sec) as committer_utc_datetime
FROM commits_with_timezone