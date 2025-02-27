-- Query to calculate BigQuery costs based on usage data
-- Completely restructured to avoid correlated subqueries

WITH 
-- Extract job statistics from the INFORMATION_SCHEMA
job_stats AS (
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
    statement_type,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    error_result,
    cache_hit,
    destination_table,
    referenced_tables,
    labels,
    priority,
    EXTRACT(HOUR FROM creation_time) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM creation_time) AS day_of_week,
    TIMESTAMP_DIFF(end_time, creation_time, SECOND) AS execution_time_seconds,
    ARRAY(
      SELECT DISTINCT 
        CONCAT(ref_table.project_id, '.', ref_table.dataset_id)
      FROM 
        UNNEST(referenced_tables) AS ref_table
      WHERE 
        ref_table.project_id IS NOT NULL 
        AND ref_table.dataset_id IS NOT NULL
    ) AS referenced_datasets,
    ARRAY(
      SELECT AS STRUCT
        ref_table.project_id,
        ref_table.dataset_id,
        ref_table.table_id,
        CONCAT(ref_table.project_id, '.', ref_table.dataset_id, '.', ref_table.table_id) AS full_table_name
      FROM 
        UNNEST(referenced_tables) AS ref_table
      WHERE 
        ref_table.project_id IS NOT NULL 
        AND ref_table.dataset_id IS NOT NULL
        AND ref_table.table_id IS NOT NULL
    ) AS referenced_tables_detail,
    (destination_table IS NOT NULL AND 
     ARRAY_LENGTH(ARRAY(
       SELECT 1 
       FROM UNNEST(referenced_tables) AS ref 
       WHERE ref.table_id = destination_table.table_id
       AND ref.dataset_id = destination_table.dataset_id
     )) > 0
    ) AS is_table_rebuild
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS
  WHERE
    creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @history_days DAY)
    AND job_type = 'QUERY'
    AND statement_type != 'SCRIPT'
),

-- Extract individual query details
query_details AS (
  SELECT
    job_id,
    creation_time,
    FORMAT_TIMESTAMP('%Y-%m-%d', creation_time) AS date,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', creation_time) AS timestamp,
    project_id,
    user_email,
    service_account,
    statement_type,
    priority,
    hour_of_day,
    day_of_week,
    execution_time_seconds,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    cache_hit,
    error_result IS NOT NULL AS has_error,
    SUBSTR(query, 0, 1000) AS query_text,
    ARRAY_LENGTH(referenced_datasets) AS num_datasets_referenced,
    ROUND(total_bytes_billed / POWER(1024, 4) * @cost_per_terabyte, 2) AS query_cost_usd,
    referenced_datasets
  FROM 
    job_stats
),

-- Calculate per-table costs
table_costs AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
    js.project_id,
    js.user_email,
    js.service_account,
    table_detail.full_table_name AS table_name,
    table_detail.project_id AS table_project,
    table_detail.dataset_id AS table_dataset,
    table_detail.table_id AS table_id,
    COUNT(*) AS query_count,
    LOGICAL_OR(js.is_table_rebuild AND 
               js.destination_table.dataset_id = table_detail.dataset_id AND 
               js.destination_table.table_id = table_detail.table_id) AS is_rebuild_operation,
    SUM(js.total_bytes_processed / ARRAY_LENGTH(js.referenced_tables_detail)) AS bytes_processed,
    SUM(js.total_bytes_billed / ARRAY_LENGTH(js.referenced_tables_detail)) AS bytes_billed,
    ROUND(SUM(js.total_bytes_billed / ARRAY_LENGTH(js.referenced_tables_detail)) / 
          POWER(1024, 4) * @cost_per_terabyte, 2) AS table_cost_usd
  FROM
    job_stats js,
    UNNEST(js.referenced_tables_detail) AS table_detail
  WHERE
    ARRAY_LENGTH(js.referenced_tables_detail) > 0
  GROUP BY
    date, js.project_id, js.user_email, js.service_account, 
    table_name, table_project, table_dataset, table_id
),

-- Calculate per-dataset costs
dataset_costs AS (
  SELECT
    tc.date,
    tc.project_id,
    tc.user_email,
    tc.service_account,
    CONCAT(tc.table_project, '.', tc.table_dataset) AS dataset,
    SUM(tc.query_count) AS query_count,
    SUM(tc.bytes_processed) AS bytes_processed,
    SUM(tc.bytes_billed) AS bytes_billed,
    SUM(tc.table_cost_usd) AS dataset_cost_usd,
    SUM(CASE WHEN tc.is_rebuild_operation THEN 1 ELSE 0 END) AS rebuild_operations
  FROM
    table_costs tc
  GROUP BY
    tc.date, tc.project_id, tc.user_email, tc.service_account, dataset
),

-- Calculate hourly aggregates
hourly_aggregates AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
    js.project_id,
    js.user_email,
    js.service_account,
    js.hour_of_day,
    COUNT(*) AS query_count,
    SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) AS cache_hit_count,
    SUM(CASE WHEN js.error_result IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    SUM(js.total_bytes_processed) AS total_bytes_processed,
    SUM(js.total_bytes_billed) AS total_bytes_billed,
    ROUND(SUM(js.total_bytes_billed) / POWER(1024, 4) * @cost_per_terabyte, 2) AS estimated_cost_usd
  FROM
    job_stats js
  GROUP BY
    date, js.project_id, js.user_email, js.service_account, js.hour_of_day
),

-- Calculate daily (weekday) aggregates
weekday_aggregates AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
    js.project_id,
    js.user_email,
    js.service_account,
    js.day_of_week,
    COUNT(*) AS query_count,
    SUM(js.total_bytes_billed) AS total_bytes_billed,
    ROUND(SUM(js.total_bytes_billed) / POWER(1024, 4) * @cost_per_terabyte, 2) AS estimated_cost_usd
  FROM
    job_stats js
  GROUP BY
    date, js.project_id, js.user_email, js.service_account, js.day_of_week
),

-- Create base user daily stats
user_daily_base AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
    js.project_id,
    js.user_email,
    js.service_account,
    -- Basic query stats
    COUNT(*) AS query_count,
    SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) AS cache_hit_count,
    SUM(CASE WHEN js.error_result IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    SUM(js.total_bytes_processed) AS total_bytes_processed,
    SUM(js.total_bytes_billed) AS total_bytes_billed,
    ROUND(SUM(js.total_bytes_billed) / POWER(1024, 4) * @cost_per_terabyte, 2) AS estimated_cost_usd,
    SUM(js.total_slot_ms) / 1000 / 3600 AS slot_hours,
    -- Cache efficiency
    ROUND(SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS cache_hit_percentage
  FROM
    job_stats js
  GROUP BY
    date, js.project_id, js.user_email, js.service_account
),

-- Hourly breakdown
user_hourly_breakdown AS (
  SELECT
    udb.date,
    udb.project_id,
    udb.user_email,
    udb.service_account,
    ARRAY_AGG(
      STRUCT(
        ha.hour_of_day, 
        ha.query_count as hourly_queries,
        ha.estimated_cost_usd as hourly_cost
      )
      ORDER BY ha.hour_of_day
    ) AS hourly_breakdown
  FROM
    user_daily_base udb
  JOIN
    hourly_aggregates ha
  ON
    ha.date = udb.date
    AND ha.project_id = udb.project_id
    AND ha.user_email = udb.user_email
    AND (ha.service_account = udb.service_account OR (ha.service_account IS NULL AND udb.service_account IS NULL))
  GROUP BY
    udb.date, udb.project_id, udb.user_email, udb.service_account
),

-- Daily breakdown
user_daily_breakdown AS (
  SELECT
    udb.date,
    udb.project_id,
    udb.user_email,
    udb.service_account,
    ARRAY_AGG(
      STRUCT(
        wa.day_of_week, 
        wa.query_count as daily_queries,
        wa.estimated_cost_usd as daily_cost
      )
      ORDER BY wa.day_of_week
    ) AS daily_breakdown
  FROM
    user_daily_base udb
  JOIN
    weekday_aggregates wa
  ON
    wa.date = udb.date
    AND wa.project_id = udb.project_id
    AND wa.user_email = udb.user_email
    AND (wa.service_account = udb.service_account OR (wa.service_account IS NULL AND udb.service_account IS NULL))
  GROUP BY
    udb.date, udb.project_id, udb.user_email, udb.service_account
),

-- Dataset costs
user_dataset_costs AS (
  SELECT
    udb.date,
    udb.project_id,
    udb.user_email,
    udb.service_account,
    ARRAY_AGG(
      STRUCT(
        dc.dataset,
        dc.bytes_processed,
        dc.bytes_billed,
        dc.dataset_cost_usd,
        dc.rebuild_operations
      )
      ORDER BY dc.dataset_cost_usd DESC
    ) AS dataset_costs
  FROM
    user_daily_base udb
  JOIN
    dataset_costs dc
  ON
    dc.date = udb.date
    AND dc.project_id = udb.project_id
    AND dc.user_email = udb.user_email
    AND (dc.service_account = udb.service_account OR (dc.service_account IS NULL AND udb.service_account IS NULL))
  GROUP BY
    udb.date, udb.project_id, udb.user_email, udb.service_account
),

-- Table costs
user_table_costs AS (
  SELECT
    udb.date,
    udb.project_id,
    udb.user_email,
    udb.service_account,
    ARRAY_AGG(
      STRUCT(
        tc.table_name,
        tc.table_id,
        CONCAT(tc.table_project, '.', tc.table_dataset) AS dataset_name,
        tc.bytes_processed,
        tc.bytes_billed,
        tc.table_cost_usd,
        tc.is_rebuild_operation,
        CASE WHEN tc.is_rebuild_operation THEN tc.table_cost_usd ELSE 0 END AS rebuild_cost_usd,
        CASE WHEN NOT tc.is_rebuild_operation THEN tc.table_cost_usd ELSE 0 END AS incremental_cost_usd,
        CASE WHEN tc.is_rebuild_operation THEN 1 ELSE 0 END AS rebuild_count
      )
      ORDER BY tc.table_cost_usd DESC
      LIMIT 100
    ) AS table_costs
  FROM
    user_daily_base udb
  JOIN
    table_costs tc
  ON
    tc.date = udb.date
    AND tc.project_id = udb.project_id
    AND tc.user_email = udb.user_email
    AND (tc.service_account = udb.service_account OR (tc.service_account IS NULL AND udb.service_account IS NULL))
  GROUP BY
    udb.date, udb.project_id, udb.user_email, udb.service_account
),

-- Recent queries
user_recent_queries AS (
  SELECT
    udb.date,
    udb.project_id,
    udb.user_email,
    udb.service_account,
    ARRAY_AGG(
      STRUCT(
        qd.job_id,
        qd.timestamp,
        qd.statement_type,
        qd.priority,
        qd.total_bytes_processed,
        qd.query_cost_usd,
        qd.execution_time_seconds,
        qd.cache_hit,
        qd.has_error,
        qd.query_text
      )
      ORDER BY qd.creation_time DESC
      LIMIT 100
    ) AS recent_queries
  FROM
    user_daily_base udb
  JOIN
    query_details qd
  ON
    qd.date = udb.date
    AND qd.project_id = udb.project_id
    AND qd.user_email = udb.user_email
    AND (qd.service_account = udb.service_account OR (qd.service_account IS NULL AND udb.service_account IS NULL))
  GROUP BY
    udb.date, udb.project_id, udb.user_email, udb.service_account
),

-- Combine all user stats
user_daily_stats AS (
  SELECT
    udb.date,
    udb.project_id,
    udb.user_email,
    udb.service_account,
    udb.query_count,
    udb.cache_hit_count,
    udb.error_count,
    udb.total_bytes_processed,
    udb.total_bytes_billed,
    udb.estimated_cost_usd,
    udb.slot_hours,
    udb.cache_hit_percentage,
    uhb.hourly_breakdown,
    udb2.daily_breakdown,
    udc.dataset_costs,
    utc.table_costs,
    urq.recent_queries
  FROM
    user_daily_base udb
  LEFT JOIN
    user_hourly_breakdown uhb
  ON
    uhb.date = udb.date
    AND uhb.project_id = udb.project_id
    AND uhb.user_email = udb.user_email
    AND (uhb.service_account = udb.service_account OR (uhb.service_account IS NULL AND udb.service_account IS NULL))
  LEFT JOIN
    user_daily_breakdown udb2
  ON
    udb2.date = udb.date
    AND udb2.project_id = udb.project_id
    AND udb2.user_email = udb.user_email
    AND (udb2.service_account = udb.service_account OR (udb2.service_account IS NULL AND udb.service_account IS NULL))
  LEFT JOIN
    user_dataset_costs udc
  ON
    udc.date = udb.date
    AND udc.project_id = udb.project_id
    AND udc.user_email = udb.user_email
    AND (udc.service_account = udb.service_account OR (udc.service_account IS NULL AND udb.service_account IS NULL))
  LEFT JOIN
    user_table_costs utc
  ON
    utc.date = udb.date
    AND utc.project_id = udb.project_id
    AND utc.user_email = udb.user_email
    AND (utc.service_account = udb.service_account OR (utc.service_account IS NULL AND udb.service_account IS NULL))
  LEFT JOIN
    user_recent_queries urq
  ON
    urq.date = udb.date
    AND urq.project_id = udb.project_id
    AND urq.user_email = udb.user_email
    AND (urq.service_account = udb.service_account OR (urq.service_account IS NULL AND udb.service_account IS NULL))
)

-- Final output
SELECT
  date,
  project_id,
  user_email,
  service_account,
  query_count,
  cache_hit_count,
  error_count,
  total_bytes_processed,
  total_bytes_billed,
  estimated_cost_usd,
  slot_hours,
  cache_hit_percentage,
  hourly_breakdown,
  daily_breakdown,
  dataset_costs,
  table_costs,
  recent_queries
FROM
  user_daily_stats
ORDER BY
  date DESC, estimated_cost_usd DESC
