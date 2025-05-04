-- big_merged_prs.sql  ·  LOC > 2000, merged 2023-01 → 2025-04
WITH events AS (
  SELECT
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.id')          AS pr_id,
    repo.name                                                  AS repo,          -- "user/reponame"
    JSON_EXTRACT_SCALAR(payload, '$.number')                   AS pr_number,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.additions') AS INT64)
      AS adds,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.deletions') AS INT64)
      AS dels,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged')      AS merged,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.diff_url')    AS diff_url_raw,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.body')        AS body,
    created_at
  FROM  `githubarchive.month.*`
  WHERE _TABLE_SUFFIX BETWEEN '202301' AND '202504'
    AND type = 'PullRequestEvent'
    AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'
)
SELECT
  SAFE_CAST(pr_id AS INT64)                                    AS pr_id,
  repo,
  SAFE_CAST(pr_number AS INT64)                                AS pr_number,
  (adds + dels)                                                AS loc,
  COALESCE(
      diff_url_raw,
      CONCAT('https://github.com/', repo, '/pull/', pr_number, '.diff')
  )                                                            AS diff_url,
  body,
  created_at
FROM events
WHERE merged = 'true'                  -- only merged PRs
  AND (adds + dels) > 2000             -- “big” PRs
LIMIT 100000;                          -- adjust as needed
