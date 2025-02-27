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
    statement_type,
    total_bytes_processed,
    total_bytes_billed,
    total_slot_ms,
    error_result,
    cache_hit,
    destination_table,
    referenced_tables,
    labels,
    -- Extract priority information
    priority,
    -- Extract hour of day for time-based analysis
    EXTRACT(HOUR FROM creation_time) AS hour_of_day,
    -- Extract day of week (1-7, 1 is Sunday)
    EXTRACT(DAYOFWEEK FROM creation_time) AS day_of_week,
    -- Query execution time in seconds
    TIMESTAMP_DIFF(end_time, creation_time, SECOND) AS execution_time_seconds,
    -- Extract dataset and table information from referenced tables
    ARRAY(
      SELECT DISTINCT 
        CONCAT(ref_table.project_id, '.', ref_table.dataset_id)
      FROM 
        UNNEST(referenced_tables) AS ref_table
      WHERE 
        ref_table.project_id IS NOT NULL 
        AND ref_table.dataset_id IS NOT NULL
    ) AS referenced_datasets,
    -- Extract full table information with table_id
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
    -- Determine if this is potentially a table rebuild operation
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

-- Extract individual query details for detailed analysis
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
    -- First 1000 characters of query for display purposes
    SUBSTR(query, 0, 1000) AS query_text,
    -- Number of datasets referenced
    ARRAY_LENGTH(referenced_datasets) AS num_datasets_referenced,
    -- Cost calculation
    ROUND(total_bytes_billed / POWER(1024, 4) * @cost_per_terabyte, 2) AS query_cost_usd,
    -- Referenced datasets
    referenced_datasets
  FROM 
    job_stats
),

-- Calculate per-table costs by attributing the cost of each query proportionally
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
    js.hour_of_day,
    js.day_of_week,
    -- Count queries referencing this table
    COUNT(*) AS query_count,
    -- Record if this table is being rebuilt (destination matches referenced)
    LOGICAL_OR(js.is_table_rebuild AND 
               js.destination_table.dataset_id = table_detail.dataset_id AND 
               js.destination_table.table_id = table_detail.table_id) AS is_rebuild_operation,
    -- Sum bytes processed, attributed proportionally if multiple tables are involved
    SUM(js.total_bytes_processed / ARRAY_LENGTH(js.referenced_tables_detail)) AS bytes_processed,
    SUM(js.total_bytes_billed / ARRAY_LENGTH(js.referenced_tables_detail)) AS bytes_billed,
    -- Calculate approximate table cost (using the configurable cost per TB)
    ROUND(SUM(js.total_bytes_billed / ARRAY_LENGTH(js.referenced_tables_detail)) / 
          POWER(1024, 4) * @cost_per_terabyte, 2) AS table_cost_usd
  FROM
    job_stats js,
    UNNEST(js.referenced_tables_detail) AS table_detail
  WHERE
    ARRAY_LENGTH(js.referenced_tables_detail) > 0
  GROUP BY
    date, js.project_id, js.user_email, js.service_account, 
    table_name, table_project, table_dataset, table_id, 
    js.hour_of_day, js.day_of_week
),

-- Calculate per-dataset costs by aggregating table costs
dataset_costs AS (
  SELECT
    tc.date,
    tc.project_id,
    tc.user_email,
    tc.service_account,
    CONCAT(tc.table_project, '.', tc.table_dataset) AS dataset,
    tc.hour_of_day,
    tc.day_of_week,
    -- Count queries referencing this dataset
    SUM(tc.query_count) AS query_count,
    -- Sum bytes processed
    SUM(tc.bytes_processed) AS bytes_processed,
    SUM(tc.bytes_billed) AS bytes_billed,
    -- Calculate approximate dataset cost
    SUM(tc.table_cost_usd) AS dataset_cost_usd,
    -- Count rebuild operations
    SUM(CASE WHEN tc.is_rebuild_operation THEN 1 ELSE 0 END) AS rebuild_operations
  FROM
    table_costs tc
  GROUP BY
    tc.date, tc.project_id, tc.user_email, tc.service_account, dataset, tc.hour_of_day, tc.day_of_week
),

-- First create a date dimension for each job to avoid GROUP BY issues
job_dates AS (
  SELECT
    job_id,
    FORMAT_TIMESTAMP('%Y-%m-%d', creation_time) AS date,
    project_id,
    user_email,
    service_account,
    hour_of_day,
    day_of_week
  FROM job_stats
),

-- Calculate daily aggregates with granular time dimensions
daily_aggregates AS (
  SELECT
    jd.date,
    jd.project_id,
    jd.user_email,
    jd.service_account,
    jd.hour_of_day,
    jd.day_of_week,
    -- Query stats
    COUNT(*) AS query_count,
    SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) AS cache_hit_count,
    SUM(CASE WHEN js.error_result IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    SUM(js.total_bytes_processed) AS total_bytes_processed,
    SUM(js.total_bytes_billed) AS total_bytes_billed,
    -- Calculate approximate cost (using the configurable cost per TB)
    ROUND(SUM(js.total_bytes_billed) / POWER(1024, 4) * @cost_per_terabyte, 2) AS estimated_cost_usd,
    SUM(js.total_slot_ms) / 1000 / 3600 AS slot_hours,
    -- Average query execution time
    AVG(js.execution_time_seconds) AS avg_execution_time_seconds,
    -- Maximum query execution time
    MAX(js.execution_time_seconds) AS max_execution_time_seconds,
    -- Calculate cache efficiency
    ROUND(SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS cache_hit_percentage,
    -- Priority distribution
    SUM(CASE WHEN js.priority = 'INTERACTIVE' THEN 1 ELSE 0 END) AS interactive_queries,
    SUM(CASE WHEN js.priority = 'BATCH' THEN 1 ELSE 0 END) AS batch_queries,
    -- Priority distribution (replacing statement type distribution to avoid multi-level aggregation)
    STRING_AGG(DISTINCT js.statement_type, ', ') AS statement_types,
    -- Add array of datasets and their costs
    ARRAY(
      SELECT AS STRUCT 
        dc.dataset, 
        SUM(dc.bytes_processed) AS bytes_processed,
        SUM(dc.bytes_billed) AS bytes_billed,
        SUM(dc.dataset_cost_usd) AS dataset_cost_usd,
        SUM(dc.rebuild_operations) AS rebuild_operations
      FROM dataset_costs dc
      WHERE 
        dc.date = jd.date
        AND dc.project_id = jd.project_id
        AND dc.user_email = jd.user_email
        AND (dc.service_account = jd.service_account OR (dc.service_account IS NULL AND jd.service_account IS NULL))
        AND dc.hour_of_day = jd.hour_of_day
        AND dc.day_of_week = jd.day_of_week
      GROUP BY dc.dataset
      ORDER BY dataset_cost_usd DESC
    ) AS dataset_costs
  FROM
    job_dates jd
  JOIN
    job_stats js ON jd.job_id = js.job_id
  GROUP BY
    jd.date, jd.project_id, jd.user_email, jd.service_account, jd.hour_of_day, jd.day_of_week
)

-- Pre-aggregate dataset costs from daily aggregates
WITH daily_agg_keys AS (
  SELECT
    date,
    project_id,
    user_email,
    service_account,
    CONCAT(date, '|', project_id, '|', user_email, '|', IFNULL(service_account, 'NULL')) AS agg_key
  FROM daily_aggregates
),

expanded_datasets AS (
  SELECT
    da.agg_key,
    ds.dataset,
    ds.bytes_processed,
    ds.bytes_billed,
    ds.dataset_cost_usd,
    ds.rebuild_operations
  FROM 
    daily_aggregates da
    CROSS JOIN UNNEST(da.dataset_costs) ds
),

aggregated_datasets AS (
  SELECT
    dak.date,
    dak.project_id,
    dak.user_email,
    dak.service_account,
    ARRAY_AGG(
      STRUCT(
        eds.dataset, 
        SUM(eds.bytes_processed) AS bytes_processed,
        SUM(eds.bytes_billed) AS bytes_billed,
        SUM(eds.dataset_cost_usd) AS dataset_cost_usd,
        SUM(eds.rebuild_operations) AS rebuild_operations
      ) 
      ORDER BY SUM(eds.dataset_cost_usd) DESC
    ) AS dataset_costs
  FROM
    daily_agg_keys dak
    JOIN expanded_datasets eds ON dak.agg_key = eds.agg_key
  GROUP BY
    dak.date, dak.project_id, dak.user_email, dak.service_account
),

-- Pre-aggregate table costs
aggregated_tables AS (
  SELECT
    tc.date,
    tc.project_id,
    tc.user_email,
    tc.service_account,
    ARRAY_AGG(
      STRUCT(
        tc.table_name,
        tc.table_id,
        CONCAT(tc.table_project, '.', tc.table_dataset) AS dataset_name,
        SUM(tc.bytes_processed) AS bytes_processed,
        SUM(tc.bytes_billed) AS bytes_billed,
        SUM(tc.table_cost_usd) AS table_cost_usd,
        LOGICAL_OR(tc.is_rebuild_operation) AS is_rebuild_operation,
        SUM(CASE WHEN tc.is_rebuild_operation THEN tc.table_cost_usd ELSE 0 END) AS rebuild_cost_usd,
        SUM(CASE WHEN NOT tc.is_rebuild_operation THEN tc.table_cost_usd ELSE 0 END) AS incremental_cost_usd,
        COUNTIF(tc.is_rebuild_operation) AS rebuild_count
      )
      ORDER BY SUM(tc.table_cost_usd) DESC
      LIMIT 100
    ) AS table_costs
  FROM
    table_costs tc
  GROUP BY
    tc.date, tc.project_id, tc.user_email, tc.service_account
),

-- Pre-aggregate recent queries
aggregated_queries AS (
  SELECT
    qd.date,
    qd.project_id,
    qd.user_email,
    qd.service_account,
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
  FROM (
    SELECT
      qd.*,
      ROW_NUMBER() OVER(
        PARTITION BY qd.date, qd.project_id, qd.user_email, 
                     IFNULL(qd.service_account, 'NULL')
        ORDER BY qd.creation_time DESC
      ) AS row_num
    FROM query_details qd
  ) qd
  WHERE qd.row_num <= 100
  GROUP BY
    qd.date, qd.project_id, qd.user_email, qd.service_account
),

-- Hourly aggregates
hourly_aggregates AS (
  SELECT
    da.date,
    da.project_id,
    da.user_email,
    da.service_account,
    ARRAY_AGG(
      STRUCT(
        da.hour_of_day, 
        da.query_count as hourly_queries,
        da.estimated_cost_usd as hourly_cost
      )
      ORDER BY da.hour_of_day
    ) AS hourly_breakdown
  FROM
    daily_aggregates da
  GROUP BY
    da.date, da.project_id, da.user_email, da.service_account
),

-- Daily (by weekday) aggregates
daily_weekday_aggregates AS (
  SELECT
    da.date,
    da.project_id,
    da.user_email,
    da.service_account,
    ARRAY_AGG(
      STRUCT(
        da.day_of_week, 
        da.query_count as daily_queries,
        da.estimated_cost_usd as daily_cost
      )
      ORDER BY da.day_of_week
    ) AS daily_breakdown
  FROM
    daily_aggregates da
  GROUP BY
    da.date, da.project_id, da.user_email, da.service_account
)

-- Main query with all pre-aggregated arrays
SELECT
  da.date,
  da.project_id,
  da.user_email,
  da.service_account,
  -- Query stats
  SUM(da.query_count) AS query_count,
  SUM(da.cache_hit_count) AS cache_hit_count,
  SUM(da.error_count) AS error_count,
  SUM(da.total_bytes_processed) AS total_bytes_processed,
  SUM(da.total_bytes_billed) AS total_bytes_billed,
  SUM(da.estimated_cost_usd) AS estimated_cost_usd,
  SUM(da.slot_hours) AS slot_hours,
  -- Calculate weighted average cache hit percentage
  ROUND(SUM(da.cache_hit_count) / NULLIF(SUM(da.query_count), 0) * 100, 2) AS cache_hit_percentage,
  -- Join pre-aggregated hourly breakdown
  ha.hourly_breakdown,
  -- Join pre-aggregated daily breakdown by weekday
  dwa.daily_breakdown,
  -- Join pre-aggregated dataset costs
  ads.dataset_costs,
  -- Join pre-aggregated table costs  
  at.table_costs,
  -- Join pre-aggregated recent queries
  aq.recent_queries
FROM
  daily_aggregates da
  LEFT JOIN hourly_aggregates ha
    ON da.date = ha.date 
    AND da.project_id = ha.project_id
    AND da.user_email = ha.user_email
    AND (da.service_account = ha.service_account 
         OR (da.service_account IS NULL AND ha.service_account IS NULL))
  LEFT JOIN daily_weekday_aggregates dwa
    ON da.date = dwa.date 
    AND da.project_id = dwa.project_id
    AND da.user_email = dwa.user_email
    AND (da.service_account = dwa.service_account 
         OR (da.service_account IS NULL AND dwa.service_account IS NULL))
  LEFT JOIN aggregated_datasets ads
    ON da.date = ads.date 
    AND da.project_id = ads.project_id
    AND da.user_email = ads.user_email
    AND (da.service_account = ads.service_account 
         OR (da.service_account IS NULL AND ads.service_account IS NULL))
  LEFT JOIN aggregated_tables at
    ON da.date = at.date 
    AND da.project_id = at.project_id
    AND da.user_email = at.user_email
    AND (da.service_account = at.service_account 
         OR (da.service_account IS NULL AND at.service_account IS NULL))
  LEFT JOIN aggregated_queries aq
    ON da.date = aq.date 
    AND da.project_id = aq.project_id
    AND da.user_email = aq.user_email
    AND (da.service_account = aq.service_account 
         OR (da.service_account IS NULL AND aq.service_account IS NULL))
GROUP BY
  da.date, da.project_id, da.user_email, da.service_account,
  ha.hourly_breakdown, dwa.daily_breakdown, ads.dataset_costs, at.table_costs, aq.recent_queries
ORDER BY
  da.date DESC, estimated_cost_usd DESC
