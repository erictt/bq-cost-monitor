/**
 * Example of how to use the bq-cost-monitor package in a Dataform project
 * 
 * This file demonstrates how to integrate the BigQuery Cost Monitor package
 * into a Dataform project with minimal configuration.
 */

// Import the BigQueryCostMonitor class from the package
const { BigQueryCostMonitor, QueryOptimization } = require("bq-cost-monitor");

// Example 1: Basic usage with minimal configuration
// Initialize with default options
const costMonitor = new BigQueryCostMonitor({
  schema: "analytics",  // Schema where tables will be created
  historyDays: 30,      // Number of days of history to include
  costPerTerabyte: 5.0  // Cost per terabyte of data processed
});

// Create all tables with one call
costMonitor.createAllTables();

// Example 2: Advanced usage with custom table names
// Initialize with custom configuration
const customCostMonitor = new BigQueryCostMonitor({
  schema: "bq_monitoring",
  historyDays: 60,
  costPerTerabyte: 6.5
});

// Create the main cost monitoring table with a custom name
const mainTable = customCostMonitor.createCostMonitoringTable({
  name: "custom_bigquery_cost_monitoring"
});

// Create the daily cost summary view with a custom name
customCostMonitor.createDailySummaryView({
  name: "custom_daily_cost_summary",
  sourceTable: mainTable.name
});

// Create the dataset cost summary view with a custom name
customCostMonitor.createDatasetSummaryView({
  name: "custom_dataset_cost_summary",
  sourceTable: mainTable.name
});

// Create the service account cost summary view with a custom name
customCostMonitor.createServiceAccountSummaryView({
  name: "custom_service_account_cost_summary",
  sourceTable: mainTable.name
});

// Create the user dataset attribution table with a custom name
customCostMonitor.createUserDatasetAttributionTable({
  name: "custom_user_dataset_attribution",
  sourceTable: mainTable.name
});

// Example 3: Using the QueryOptimization utilities
// Define a SQL query
const myQuery = `
SELECT
  user_id,
  COUNT(*) as event_count
FROM
  \${ref("events")}
GROUP BY
  user_id
ORDER BY
  event_count DESC
`;

// Add optimization tips to the query
const queryWithTips = QueryOptimization.optimizationTips() + myQuery;

// Add cost estimation to the query
const queryWithCost = QueryOptimization.addCostEstimation(myQuery, {
  estimatedTB: 1.5,
  costPerTB: 5.0
});

// Add query tagging
const taggedQuery = QueryOptimization.addQueryTagging(
  myQuery,
  "my-project",
  "data-team",
  {
    owner: "data-engineer",
    purpose: "daily reporting"
  }
);

// Add partition filter to the query
const filteredQuery = QueryOptimization.addPartitionFilter(
  myQuery,
  "date",
  ">= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
);

// Make a query incremental
const incrementalQuery = QueryOptimization.makeIncremental(
  myQuery,
  "last_modified_date",
  ">= '${dataform.projectConfig.vars.lastRunTime}'"
);

// Example 4: Creating a custom table using the query optimization utilities
// Create a table with the optimized query
publish("analytics", "optimized_events_summary")
  .type("table")
  .description("Optimized events summary with cost monitoring")
  .query(ctx => QueryOptimization.addPartitionFilter(
    QueryOptimization.addCostEstimation(myQuery, { estimatedTB: 0.5 }),
    "date",
    ">= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
  ));
