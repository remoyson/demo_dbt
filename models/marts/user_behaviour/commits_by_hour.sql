{{
    config(
        materialized='table'
    )
}}
WITH commits_hour as
(
    SELECT EXTRACT(HOUR FROM committer_local_datetime) as commit_local_hour
    FROM {{ref('commits_enhanced')}}
)

SELECT commit_local_hour, count(1) as n_of_commits
FROM commits_hour
GROUP BY commit_local_hour
ORDER BY commit_local_hour ASC
