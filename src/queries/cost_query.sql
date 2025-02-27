-- Query to calculate BigQuery costs based on usage data
-- This query aggregates usage data and applies cost calculations with dataset and service account tracking

WITH job_stats AS (
  SELECT
    project_id,
    user_email,
    -- Extract service account info from user_email if present
    CASE 
      WHEN user_email LIKE '%.gserviceaccount.com' THEN user_email
      WHEN user_email LIKE 'service-%' THEN user_email
      ELSE NULL
    END AS service_account,
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
    labels,
    -- Extract dataset information from referenced tables
    ARRAY(
      SELECT DISTINCT 
        CONCAT(ref_table.project_id, '.', ref_table.dataset_id)
      FROM 
        UNNEST(referenced_tables) AS ref_table
      WHERE 
        ref_table.project_id IS NOT NULL 
        AND ref_table.dataset_id IS NOT NULL
    ) AS referenced_datasets
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @history_days DAY)
    AND job_type = 'QUERY'
    AND statement_type != 'SCRIPT'
),

-- Calculate per-dataset costs by attributing the cost of each query proportionally
dataset_costs AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', creation_time) AS date,
    project_id,
    user_email,
    service_account,
    dataset,
    -- Count queries referencing this dataset
    COUNT(*) AS query_count,
    -- Sum bytes processed, attributed proportionally if multiple datasets are involved
    SUM(total_bytes_processed / ARRAY_LENGTH(referenced_datasets)) AS bytes_processed,
    SUM(total_bytes_billed / ARRAY_LENGTH(referenced_datasets)) AS bytes_billed,
    -- Calculate approximate dataset cost (using the configurable cost per TB)
    ROUND(SUM(total_bytes_billed / ARRAY_LENGTH(referenced_datasets)) / 
          POWER(1024, 4) * @cost_per_terabyte, 2) AS dataset_cost_usd
  FROM
    job_stats,
    UNNEST(referenced_datasets) AS dataset
  WHERE
    ARRAY_LENGTH(referenced_datasets) > 0
  GROUP BY
    date, project_id, user_email, service_account, dataset
)

-- Main query with user and service account attribution
SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
  js.project_id,
  js.user_email,
  js.service_account,
  -- Query stats
  COUNT(*) AS query_count,
  SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) AS cache_hit_count,
  SUM(CASE WHEN js.error_result IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
  SUM(js.total_bytes_processed) AS total_bytes_processed,
  SUM(js.total_bytes_billed) AS total_bytes_billed,
  -- Calculate approximate cost (using the configurable cost per TB)
  ROUND(SUM(js.total_bytes_billed) / POWER(1024, 4) * @cost_per_terabyte, 2) AS estimated_cost_usd,
  SUM(js.total_slot_ms) / 1000 / 3600 AS slot_hours,
  -- Calculate cache efficiency
  ROUND(SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS cache_hit_percentage,
  -- Add array of datasets and their costs
  ARRAY(
    SELECT AS STRUCT 
      dc.dataset, 
      dc.bytes_processed,
      dc.bytes_billed,
      dc.dataset_cost_usd
    FROM dataset_costs dc
    WHERE 
      dc.date = FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time)
      AND dc.project_id = js.project_id
      AND dc.user_email = js.user_email
      AND (dc.service_account = js.service_account OR (dc.service_account IS NULL AND js.service_account IS NULL))
    ORDER BY dc.dataset_cost_usd DESC
  ) AS dataset_costs
FROM
  job_stats js
GROUP BY
  date, project_id, user_email, service_account
ORDER BY
  date DESC, estimated_cost_usd DESC
