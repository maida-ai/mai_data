-- PullRequestEvents  ❚  merged  ❚  LOC > 2 000  ❚  April 2025 only
-- Result: 1 000 rows  →  export as NDJSON for Maida AI pipeline

SELECT
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.id')        AS INT64)  AS pr_id,
  repo.name                                                                       AS repo,         -- "owner/repo"
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.number')               AS INT64)  AS pr_number,
  ( SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.additions') AS INT64) +
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.deletions') AS INT64) )                AS loc,
  COALESCE(
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.diff_url'),
    CONCAT('https://github.com/', repo.name, '/pull/',
           JSON_EXTRACT_SCALAR(payload, '$.number'), '.diff')
  )                                                                                AS diff_url,
  JSON_EXTRACT_SCALAR(payload, '$.pull_request.body')                              AS body,
  created_at
FROM  `githubarchive.month.202504`                       -- April 2025 shard
WHERE type = 'PullRequestEvent'
  AND JSON_EXTRACT_SCALAR(payload, '$.action')  = 'closed'
  AND JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged') = 'true'
  AND ( SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.additions') AS INT64) +
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.deletions') AS INT64) ) > 2000
LIMIT 1000;                                              -- tiny slice for testing
