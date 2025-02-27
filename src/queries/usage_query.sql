-- Query to extract BigQuery usage information from INFORMATION_SCHEMA.JOBS
-- This query focuses on data processed, which is the primary cost driver

SELECT
  project_id,
  user_email,
  job_id,
  creation_time,
  end_time,
  TIMESTAMP_DIFF(end_time, creation_time, SECOND) AS duration_seconds,
  query,
  total_bytes_processed,
  total_bytes_billed,
  total_slot_ms,
  error_result,
  cache_hit,
  destination_table,
  referenced_tables,
  labels
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @history_days DAY)
  AND job_type = 'QUERY'
  AND statement_type != 'SCRIPT'
ORDER BY
  creation_time DESC
