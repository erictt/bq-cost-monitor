/**
 * BigQuery Cost Monitoring Table Definition
 * 
 * This module defines the main cost monitoring table that collects and analyzes
 * BigQuery usage data from INFORMATION_SCHEMA.JOBS.
 */

/**
 * Create the main cost monitoring table
 * @param {Object} ctx - The dataform context object
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the table in
 * @param {string} options.name - The name of the table
 * @param {number} options.historyDays - Number of days of history to include
 * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
 * @returns {Object} - The created table declaration
 */
function createCostMonitoringTable(ctx, options) {
  const {
    schema,
    name,
    historyDays,
    costPerTerabyte
  } = options;
  
  // Create the table
  const table = ctx.publish(schema, name)
    .type("table")
    .description("Enhanced table containing BigQuery usage and cost data with granular insights")
    .bigquery({
      partitionBy: "date",
      clusterBy: ["project_id", "user_email"]
    })
    .tags(["cost", "monitoring", "service-accounts", "datasets", "analytics"]);
  
  // Add column descriptions
  table.columns({
    date: "Date of the query execution (YYYY-MM-DD)",
    project_id: "Google Cloud project ID",
    user_email: "Email of the user who executed the query",
    service_account: "Service account identifier if the query was executed by a service account",
    query_count: "Number of queries executed",
    cache_hit_count: "Number of queries that used cached results",
    error_count: "Number of queries that resulted in errors",
    total_bytes_processed: "Total bytes processed by all queries",
    total_bytes_billed: "Total bytes billed for all queries",
    estimated_cost_usd: "Estimated cost in USD based on bytes billed",
    slot_hours: "Total slot hours consumed",
    cache_hit_percentage: "Percentage of queries that used cached results",
    dataset_costs: "Array of datasets used with their associated costs"
  });
  
  // Set the query
  table.query(ctx => `
    -- Completely restructured SQL to avoid correlated subqueries
    WITH 
    -- Extract basic job information
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
        total_bytes_processed,
        total_bytes_billed,
        total_slot_ms,
        error_result,
        cache_hit,
        referenced_tables,
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
        \${ref("INFORMATION_SCHEMA.JOBS")}
      WHERE
        creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${historyDays} DAY)
        AND job_type = 'QUERY'
        AND statement_type != 'SCRIPT'
    ),

    -- Calculate per-dataset costs
    dataset_costs AS (
      SELECT
        FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
        js.project_id,
        js.user_email,
        js.service_account,
        dataset,
        -- Count queries referencing this dataset
        COUNT(*) AS query_count,
        -- Sum bytes processed, attributed proportionally if multiple datasets are involved
        SUM(js.total_bytes_processed / ARRAY_LENGTH(js.referenced_datasets)) AS bytes_processed,
        SUM(js.total_bytes_billed / ARRAY_LENGTH(js.referenced_datasets)) AS bytes_billed,
        -- Calculate approximate dataset cost
        ROUND(SUM(js.total_bytes_billed / ARRAY_LENGTH(js.referenced_datasets)) / 
              POWER(1024, 4) * ${costPerTerabyte}, 2) AS dataset_cost_usd
      FROM
        job_stats js,
        UNNEST(js.referenced_datasets) AS dataset
      WHERE
        ARRAY_LENGTH(js.referenced_datasets) > 0
      GROUP BY
        date, js.project_id, js.user_email, js.service_account, dataset
    ),

    -- Precompute aggregated dataset costs
    aggregated_dataset_costs AS (
      SELECT
        dc.date,
        dc.project_id,
        dc.user_email,
        dc.service_account,
        ARRAY_AGG(
          STRUCT(
            dc.dataset,
            dc.bytes_processed,
            dc.bytes_billed,
            dc.dataset_cost_usd
          )
          ORDER BY dc.dataset_cost_usd DESC
        ) AS dataset_costs
      FROM
        dataset_costs dc
      GROUP BY
        dc.date, dc.project_id, dc.user_email, dc.service_account
    ),

    -- Compute user daily stats
    user_stats AS (
      SELECT
        FORMAT_TIMESTAMP('%Y-%m-%d', js.creation_time) AS date,
        js.project_id,
        js.user_email,
        js.service_account,
        COUNT(*) AS query_count,
        SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) AS cache_hit_count,
        SUM(CASE WHEN js.error_result IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
        SUM(js.total_bytes_processed) AS total_bytes_processed,
        SUM(js.total_bytes_billed) AS total_bytes_billed,
        ROUND(SUM(js.total_bytes_billed) / POWER(1024, 4) * ${costPerTerabyte}, 2) AS estimated_cost_usd,
        SUM(js.total_slot_ms) / 1000 / 3600 AS slot_hours,
        ROUND(SUM(CASE WHEN js.cache_hit THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS cache_hit_percentage
      FROM
        job_stats js
      GROUP BY
        date, js.project_id, js.user_email, js.service_account
    )

    -- Final join of user stats with dataset costs
    SELECT
      us.date,
      us.project_id,
      us.user_email,
      us.service_account,
      us.query_count,
      us.cache_hit_count,
      us.error_count,
      us.total_bytes_processed,
      us.total_bytes_billed,
      us.estimated_cost_usd,
      us.slot_hours,
      us.cache_hit_percentage,
      -- Join the pre-aggregated dataset costs
      IFNULL(adc.dataset_costs, []) AS dataset_costs
    FROM
      user_stats us
    LEFT JOIN
      aggregated_dataset_costs adc
      ON us.date = adc.date
      AND us.project_id = adc.project_id
      AND us.user_email = adc.user_email
      AND (us.service_account = adc.service_account OR (us.service_account IS NULL AND adc.service_account IS NULL))
    ORDER BY
      us.date DESC, us.estimated_cost_usd DESC
  `);
  
  return table;
}

module.exports = createCostMonitoringTable;
