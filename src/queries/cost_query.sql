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
    FORMAT_TIMESTAMP('%Y-%m-%d', creation_time) AS date,
    project_id,
    user_email,
    service_account,
    table_detail.full_table_name AS table_name,
    table_detail.project_id AS table_project,
    table_detail.dataset_id AS table_dataset,
    table_detail.table_id AS table_id,
    hour_of_day,
    day_of_week,
    -- Count queries referencing this table
    COUNT(*) AS query_count,
    -- Record if this table is being rebuilt (destination matches referenced)
    LOGICAL_OR(is_table_rebuild AND 
               destination_table.dataset_id = table_detail.dataset_id AND 
               destination_table.table_id = table_detail.table_id) AS is_rebuild_operation,
    -- Sum bytes processed, attributed proportionally if multiple tables are involved
    SUM(total_bytes_processed / ARRAY_LENGTH(referenced_tables_detail)) AS bytes_processed,
    SUM(total_bytes_billed / ARRAY_LENGTH(referenced_tables_detail)) AS bytes_billed,
    -- Calculate approximate table cost (using the configurable cost per TB)
    ROUND(SUM(total_bytes_billed / ARRAY_LENGTH(referenced_tables_detail)) / 
          POWER(1024, 4) * @cost_per_terabyte, 2) AS table_cost_usd
  FROM
    job_stats,
    UNNEST(referenced_tables_detail) AS table_detail
  WHERE
    ARRAY_LENGTH(referenced_tables_detail) > 0
  GROUP BY
    date, project_id, user_email, service_account, 
    table_name, table_project, table_dataset, table_id, 
    hour_of_day, day_of_week
),

-- Calculate per-dataset costs by aggregating table costs
dataset_costs AS (
  SELECT
    date,
    project_id,
    user_email,
    service_account,
    CONCAT(table_project, '.', table_dataset) AS dataset,
    hour_of_day,
    day_of_week,
    -- Count queries referencing this dataset
    SUM(query_count) AS query_count,
    -- Sum bytes processed
    SUM(bytes_processed) AS bytes_processed,
    SUM(bytes_billed) AS bytes_billed,
    -- Calculate approximate dataset cost
    SUM(table_cost_usd) AS dataset_cost_usd,
    -- Count rebuild operations
    SUM(CASE WHEN is_rebuild_operation THEN 1 ELSE 0 END) AS rebuild_operations
  FROM
    table_costs
  GROUP BY
    date, project_id, user_email, service_account, dataset, hour_of_day, day_of_week
),

-- Calculate daily aggregates with granular time dimensions
daily_aggregates AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
    js.project_id,
    js.user_email,
    js.service_account,
    js.hour_of_day,
    js.day_of_week,
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
    -- Statement type distribution
    ARRAY_AGG(STRUCT(js.statement_type AS type, COUNT(*) AS count) GROUP BY js.statement_type) AS statement_types,
    -- Add array of datasets and their costs
    ARRAY(
      SELECT AS STRUCT 
        dc.dataset, 
        SUM(dc.bytes_processed) AS bytes_processed,
        SUM(dc.bytes_billed) AS bytes_billed,
        SUM(dc.dataset_cost_usd) AS dataset_cost_usd
      FROM dataset_costs dc
      WHERE 
        dc.date = FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time)
        AND dc.project_id = js.project_id
        AND dc.user_email = js.user_email
        AND (dc.service_account = js.service_account OR (dc.service_account IS NULL AND js.service_account IS NULL))
        AND dc.hour_of_day = js.hour_of_day
        AND dc.day_of_week = js.day_of_week
      GROUP BY dc.dataset
      ORDER BY dataset_cost_usd DESC
    ) AS dataset_costs
  FROM
    job_stats js
  GROUP BY
    date, project_id, user_email, service_account, hour_of_day, day_of_week
)

-- Main query with user and service account attribution, aggregating by date only for the main report
SELECT
  date,
  project_id,
  user_email,
  service_account,
  -- Query stats
  SUM(query_count) AS query_count,
  SUM(cache_hit_count) AS cache_hit_count,
  SUM(error_count) AS error_count,
  SUM(total_bytes_processed) AS total_bytes_processed,
  SUM(total_bytes_billed) AS total_bytes_billed,
  SUM(estimated_cost_usd) AS estimated_cost_usd,
  SUM(slot_hours) AS slot_hours,
  -- Calculate weighted average cache hit percentage
  ROUND(SUM(cache_hit_count) / NULLIF(SUM(query_count), 0) * 100, 2) AS cache_hit_percentage,
  -- Store hourly breakdown for time pattern analysis
  ARRAY_AGG(
    STRUCT(
      hour_of_day, 
      query_count as hourly_queries,
      estimated_cost_usd as hourly_cost
    )
    ORDER BY hour_of_day
  ) AS hourly_breakdown,
  -- Store daily breakdown for weekday pattern analysis
  ARRAY_AGG(
    STRUCT(
      day_of_week, 
      query_count as daily_queries,
      estimated_cost_usd as daily_cost
    )
    ORDER BY day_of_week
  ) AS daily_breakdown,
  -- Combine all dataset costs across hours
  ARRAY(
    SELECT AS STRUCT
      ds.dataset,
      SUM(ds.bytes_processed) AS bytes_processed,
      SUM(ds.bytes_billed) AS bytes_billed,
      SUM(ds.dataset_cost_usd) AS dataset_cost_usd,
      SUM(ds.rebuild_operations) AS rebuild_operations
    FROM daily_aggregates d, UNNEST(d.dataset_costs) AS ds
    WHERE 
      d.date = date
      AND d.project_id = project_id
      AND d.user_email = user_email
      AND (d.service_account = service_account OR (d.service_account IS NULL AND service_account IS NULL))
    GROUP BY ds.dataset
    ORDER BY dataset_cost_usd DESC
  ) AS dataset_costs,
  
  -- Include all table costs with rebuild information
  ARRAY(
    SELECT AS STRUCT
      tc.table_name,
      tc.table_id,
      CONCAT(tc.table_project, '.', tc.table_dataset) AS dataset_name,
      SUM(tc.bytes_processed) AS bytes_processed,
      SUM(tc.bytes_billed) AS bytes_billed,
      SUM(tc.table_cost_usd) AS table_cost_usd,
      LOGICAL_OR(tc.is_rebuild_operation) AS is_rebuild_operation,
      SUM(CASE WHEN tc.is_rebuild_operation THEN tc.table_cost_usd ELSE 0 END) AS rebuild_cost_usd,
      SUM(CASE WHEN NOT tc.is_rebuild_operation THEN tc.table_cost_usd ELSE 0 END) AS incremental_cost_usd,
      COUNT(DISTINCT CASE WHEN tc.is_rebuild_operation THEN CONCAT(tc.date, tc.hour_of_day) END) AS rebuild_count
    FROM table_costs tc
    WHERE 
      tc.date = date
      AND tc.project_id = project_id
      AND tc.user_email = user_email
      AND (tc.service_account = service_account OR (tc.service_account IS NULL AND service_account IS NULL))
    GROUP BY 
      tc.table_name, tc.table_id, dataset_name
    ORDER BY 
      table_cost_usd DESC
    LIMIT 100
  ) AS table_costs,
  -- Include all individual queries for detailed exploration
  (
    SELECT ARRAY_AGG(
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
        qd.query_text,
        qd.referenced_datasets
      )
      ORDER BY qd.creation_time DESC
      LIMIT 100  -- Keep the 100 most recent queries per day/user combination
    )
    FROM query_details qd
    WHERE 
      qd.date = date
      AND qd.project_id = project_id
      AND qd.user_email = user_email
      AND (qd.service_account = service_account OR (qd.service_account IS NULL AND service_account IS NULL))
  ) AS recent_queries
FROM
  daily_aggregates
GROUP BY
  date, project_id, user_email, service_account
ORDER BY
  date DESC, estimated_cost_usd DESC
