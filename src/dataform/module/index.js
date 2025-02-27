/**
 * BigQuery Cost Monitor - Dataform Module
 * 
 * This module provides dataform definitions for BigQuery cost monitoring.
 * It can be imported into other dataform projects to add cost monitoring capabilities.
 */

const queryOptimization = require('./includes/query_optimization');

/**
 * Create the main cost monitoring table
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the table in
 * @param {string} options.name - The name of the table
 * @param {number} options.historyDays - Number of days of history to include
 * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
 * @returns {Object} - The created table declaration
 */
function createCostMonitoringTable(options = {}) {
  const {
    schema = 'analytics',
    name = 'bigquery_cost_monitoring',
    historyDays = 30,
    costPerTerabyte = 5.0
  } = options;
  
  // Create the table
  const table = publish(schema, name)
    .type('table')
    .description('Enhanced table containing BigQuery usage and cost data with granular insights')
    .bigquery({
      partitionBy: 'date',
      clusterBy: ['project_id', 'user_email']
    })
    .tags(['cost', 'monitoring', 'service-accounts', 'datasets', 'analytics']);
  
  // Add column descriptions
  table.columns({
    date: 'Date of the query execution (YYYY-MM-DD)',
    project_id: 'Google Cloud project ID',
    user_email: 'Email of the user who executed the query',
    service_account: 'Service account identifier if the query was executed by a service account',
    query_count: 'Number of queries executed',
    cache_hit_count: 'Number of queries that used cached results',
    error_count: 'Number of queries that resulted in errors',
    total_bytes_processed: 'Total bytes processed by all queries',
    total_bytes_billed: 'Total bytes billed for all queries',
    estimated_cost_usd: 'Estimated cost in USD based on bytes billed',
    slot_hours: 'Total slot hours consumed',
    cache_hit_percentage: 'Percentage of queries that used cached results',
    dataset_costs: 'Array of datasets used with their associated costs'
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

/**
 * Create the daily cost summary view
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the view in
 * @param {string} options.name - The name of the view
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created view declaration
 */
function createDailyCostSummaryView(options = {}) {
  const {
    schema = 'analytics',
    name = 'daily_cost_summary',
    sourceTable = 'bigquery_cost_monitoring'
  } = options;
  
  // Create the view
  const view = publish(schema, name)
    .type('view')
    .description('Daily summary of BigQuery costs across all projects')
    .tags(['cost', 'monitoring', 'summary']);
  
  // Add column descriptions
  view.columns({
    date: 'Date of the query execution (YYYY-MM-DD)',
    project_id: 'Google Cloud project ID',
    total_queries: 'Total number of queries executed',
    total_cache_hits: 'Total number of queries that used cached results',
    total_errors: 'Total number of queries that resulted in errors',
    total_bytes_processed: 'Total bytes processed by all queries',
    total_bytes_billed: 'Total bytes billed for all queries',
    total_estimated_cost_usd: 'Total estimated cost in USD based on bytes billed',
    total_slot_hours: 'Total slot hours consumed',
    avg_cache_hit_percentage: 'Average percentage of queries that used cached results'
  });
  
  // Set the query
  view.query(ctx => `
    SELECT
      date,
      project_id,
      SUM(query_count) AS total_queries,
      SUM(cache_hit_count) AS total_cache_hits,
      SUM(error_count) AS total_errors,
      SUM(total_bytes_processed) AS total_bytes_processed,
      SUM(total_bytes_billed) AS total_bytes_billed,
      SUM(estimated_cost_usd) AS total_estimated_cost_usd,
      SUM(slot_hours) AS total_slot_hours,
      ROUND(AVG(cache_hit_percentage), 2) AS avg_cache_hit_percentage
    FROM
      \${ref(schema + "." + sourceTable)}
    GROUP BY
      date, project_id
    ORDER BY
      date DESC, total_estimated_cost_usd DESC
  `);
  
  return view;
}

/**
 * Create the dataset cost summary view
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the view in
 * @param {string} options.name - The name of the view
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created view declaration
 */
function createDatasetCostSummaryView(options = {}) {
  const {
    schema = 'analytics',
    name = 'dataset_cost_summary',
    sourceTable = 'bigquery_cost_monitoring'
  } = options;
  
  // Create the view
  const view = publish(schema, name)
    .type('view')
    .description('Summary of BigQuery costs by dataset')
    .bigquery({
      partitionBy: 'date'
    })
    .tags(['cost', 'monitoring', 'datasets']);
  
  // Add column descriptions
  view.columns({
    date: 'Date of the query execution (YYYY-MM-DD)',
    project_id: 'Google Cloud project ID',
    dataset_name: 'Full name of the dataset (project.dataset)',
    total_queries: 'Number of queries accessing this dataset',
    total_bytes_processed: 'Total bytes processed accessing this dataset',
    total_bytes_billed: 'Total bytes billed for access to this dataset',
    total_cost_usd: 'Total cost in USD attributed to this dataset',
    top_users: 'Array of users/service accounts with highest usage of this dataset'
  });
  
  // Set the query
  view.query(ctx => `
    -- Dataset cost summary
    WITH dataset_usage AS (
      SELECT
        costs.date,
        costs.project_id,
        dataset_entry.dataset AS dataset_name,
        costs.user_email,
        costs.service_account,
        dataset_entry.dataset_cost_usd,
        dataset_entry.bytes_processed,
        dataset_entry.bytes_billed
      FROM 
        \${ref(schema + "." + sourceTable)} costs,
        UNNEST(costs.dataset_costs) AS dataset_entry
    )

    SELECT
      date,
      project_id,
      dataset_name,
      COUNT(DISTINCT CONCAT(user_email, IFNULL(service_account, ''))) AS unique_users,
      SUM(bytes_processed) AS total_bytes_processed,
      SUM(bytes_billed) AS total_bytes_billed,
      SUM(dataset_cost_usd) AS total_cost_usd,
      -- Get top users for this dataset
      ARRAY(
        SELECT AS STRUCT
          COALESCE(service_account, user_email) AS user,
          SUM(dataset_cost_usd) AS user_cost_usd,
          SUM(bytes_processed) AS user_bytes_processed
        FROM dataset_usage du
        WHERE
          du.date = main.date
          AND du.project_id = main.project_id
          AND du.dataset_name = main.dataset_name
        GROUP BY
          user
        ORDER BY
          user_cost_usd DESC
        LIMIT 10
      ) AS top_users
    FROM
      dataset_usage main
    GROUP BY
      date, project_id, dataset_name
    ORDER BY
      date DESC, total_cost_usd DESC
  `);
  
  return view;
}

/**
 * Create the service account cost summary view
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the view in
 * @param {string} options.name - The name of the view
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created view declaration
 */
function createServiceAccountCostSummaryView(options = {}) {
  const {
    schema = 'analytics',
    name = 'service_account_cost_summary',
    sourceTable = 'bigquery_cost_monitoring'
  } = options;
  
  // Create the view
  const view = publish(schema, name)
    .type('view')
    .description('Summary of BigQuery costs by service account')
    .bigquery({
      partitionBy: 'date'
    })
    .tags(['cost', 'monitoring', 'service-accounts']);
  
  // Add column descriptions
  view.columns({
    date: 'Date of the query execution (YYYY-MM-DD)',
    project_id: 'Google Cloud project ID',
    service_account: 'Service account that executed the queries',
    total_queries: 'Number of queries executed by the service account',
    total_bytes_processed: 'Total bytes processed by the service account',
    total_bytes_billed: 'Total bytes billed for the service account',
    total_cost_usd: 'Total cost in USD attributed to the service account',
    top_datasets: 'Array of most expensive datasets used by this service account'
  });
  
  // Set the query
  view.query(ctx => `
    -- Service account cost summary
    SELECT
      date,
      project_id,
      service_account,
      SUM(query_count) AS total_queries,
      SUM(total_bytes_processed) AS total_bytes_processed,
      SUM(total_bytes_billed) AS total_bytes_billed,
      SUM(estimated_cost_usd) AS total_cost_usd,
      -- Get aggregated dataset costs for this service account
      ARRAY(
        SELECT AS STRUCT
          dataset_entry.dataset AS dataset_name,
          SUM(dataset_entry.dataset_cost_usd) AS dataset_cost_usd,
          SUM(dataset_entry.bytes_processed) AS dataset_bytes_processed
        FROM 
          \${ref(schema + "." + sourceTable)} costs,
          UNNEST(costs.dataset_costs) AS dataset_entry
        WHERE
          costs.date = main.date
          AND costs.project_id = main.project_id
          AND costs.service_account = main.service_account
        GROUP BY
          dataset_name
        ORDER BY
          dataset_cost_usd DESC
        LIMIT 10
      ) AS top_datasets
    FROM
      \${ref(schema + "." + sourceTable)} main
    WHERE
      service_account IS NOT NULL
    GROUP BY
      date, project_id, service_account
    ORDER BY
      date DESC, total_cost_usd DESC
  `);
  
  return view;
}

/**
 * Create the user dataset attribution table
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the table in
 * @param {string} options.name - The name of the table
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created table declaration
 */
function createUserDatasetAttributionTable(options = {}) {
  const {
    schema = 'analytics',
    name = 'user_dataset_attribution',
    sourceTable = 'bigquery_cost_monitoring'
  } = options;
  
  // Create the table
  const table = publish(schema, name)
    .type('table')
    .description('Cross-tabulation of which users/service accounts are querying which datasets and the associated costs')
    .bigquery({
      partitionBy: 'date',
      clusterBy: ['user', 'dataset_name']
    })
    .tags(['cost', 'monitoring', 'datasets', 'users', 'attribution']);
  
  // Add column descriptions
  table.columns({
    date: 'Date of the query execution (YYYY-MM-DD)',
    project_id: 'Google Cloud project ID',
    user: 'User email or service account that executed the queries',
    is_service_account: 'Whether this is a service account or a regular user',
    dataset_name: 'Dataset name (project.dataset)',
    query_count: 'Number of queries executed against this dataset',
    bytes_processed: 'Total bytes processed',
    cost_usd: 'Total cost in USD',
    top_tables: 'Most expensive tables in this dataset accessed by this user'
  });
  
  // Set the query
  table.query(ctx => `
    -- Extract user-dataset relationships from the cost monitoring data
    WITH user_dataset_data AS (
      SELECT
        cm.date,
        cm.project_id,
        -- Normalize user identifier
        COALESCE(cm.service_account, cm.user_email) AS user,
        -- Identify if this is a service account
        (cm.service_account IS NOT NULL) AS is_service_account,
        -- Extract dataset information
        ds.dataset AS dataset_name,
        ds.bytes_processed,
        ds.dataset_cost_usd AS cost_usd,
        ds.query_count
      FROM 
        \${ref(schema + "." + sourceTable)} cm,
        UNNEST(cm.dataset_costs) AS ds
    ),

    -- Extract user-table relationships
    user_table_data AS (
      SELECT
        cm.date,
        cm.project_id,
        -- Normalize user identifier
        COALESCE(cm.service_account, cm.user_email) AS user,
        -- Identify if this is a service account
        (cm.service_account IS NOT NULL) AS is_service_account,
        -- Extract table information
        t.dataset_name,
        t.table_name,
        t.table_id,
        t.bytes_processed,
        t.table_cost_usd AS cost_usd
      FROM 
        \${ref(schema + "." + sourceTable)} cm,
        UNNEST(cm.table_costs) AS t
    )

    -- Main attribution table
    SELECT
      ud.date,
      ud.project_id,
      ud.user,
      ud.is_service_account,
      ud.dataset_name,
      SUM(ud.query_count) AS query_count,
      SUM(ud.bytes_processed) AS bytes_processed,
      SUM(ud.cost_usd) AS cost_usd,
      -- Include top tables accessed by this user in this dataset
      ARRAY(
        SELECT AS STRUCT
          ut.table_name,
          ut.table_id,
          SUM(ut.cost_usd) AS table_cost_usd,
          SUM(ut.bytes_processed) AS bytes_processed
        FROM 
          user_table_data ut
        WHERE
          ut.date = ud.date
          AND ut.user = ud.user
          AND ut.dataset_name = ud.dataset_name
        GROUP BY
          ut.table_name, ut.table_id
        ORDER BY
          table_cost_usd DESC
        LIMIT 10
      ) AS top_tables,
      -- Calculate percentage of user's total cost that comes from this dataset
      ROUND(
        SUM(ud.cost_usd) / (
          SELECT SUM(ud2.cost_usd) 
          FROM user_dataset_data ud2 
          WHERE 
            ud2.date = ud.date 
            AND ud2.user = ud.user
        ) * 100, 
        2
      ) AS pct_of_user_cost,
      -- Calculate percentage of dataset's total cost that comes from this user
      ROUND(
        SUM(ud.cost_usd) / (
          SELECT SUM(ud3.cost_usd) 
          FROM user_dataset_data ud3 
          WHERE 
            ud3.date = ud.date 
            AND ud3.dataset_name = ud.dataset_name
        ) * 100, 
        2
      ) AS pct_of_dataset_cost
    FROM
      user_dataset_data ud
    GROUP BY
      date, project_id, user, is_service_account, dataset_name
    ORDER BY
      date DESC, cost_usd DESC
  `);
  
  return table;
}

/**
 * Create all cost monitoring tables and views
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the objects in
 * @param {number} options.historyDays - Number of days of history to include
 * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
 */
function createAllCostMonitoringObjects(options = {}) {
  const {
    schema = 'analytics',
    historyDays = 30,
    costPerTerabyte = 5.0
  } = options;
  
  // Create the main table
  const mainTable = createCostMonitoringTable({
    schema,
    historyDays,
    costPerTerabyte
  });
  
  // Create the views
  createDailyCostSummaryView({ schema, sourceTable: mainTable.name });
  createDatasetCostSummaryView({ schema, sourceTable: mainTable.name });
  createServiceAccountCostSummaryView({ schema, sourceTable: mainTable.name });
  createUserDatasetAttributionTable({ schema, sourceTable: mainTable.name });
}

module.exports = {
  createCostMonitoringTable,
  createDailyCostSummaryView,
  createDatasetCostSummaryView,
  createServiceAccountCostSummaryView,
  createUserDatasetAttributionTable,
  createAllCostMonitoringObjects,
  queryOptimization
};
