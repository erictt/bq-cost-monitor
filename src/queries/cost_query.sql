-- Query to calculate BigQuery costs based on usage data
-- This query aggregates usage data and applies cost calculations

WITH job_stats AS (
  SELECT
    project_id,
    user_email,
    job_id,
    creation_time,
    end_time,
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
)

SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d', creation_time) AS date,
  project_id,
  user_email,
  COUNT(*) AS query_count,
  SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END) AS cache_hit_count,
  SUM(CASE WHEN error_result IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
  SUM(total_bytes_processed) AS total_bytes_processed,
  SUM(total_bytes_billed) AS total_bytes_billed,
  -- Calculate approximate cost (using the configurable cost per TB)
  ROUND(SUM(total_bytes_billed) / POWER(1024, 4) * @cost_per_terabyte, 2) AS estimated_cost_usd,
  SUM(total_slot_ms) / 1000 / 3600 AS slot_hours,
  -- Calculate cache efficiency
  ROUND(SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS cache_hit_percentage
FROM
  job_stats
GROUP BY
  date, project_id, user_email
ORDER BY
  date DESC, estimated_cost_usd DESC
