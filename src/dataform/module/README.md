# BigQuery Cost Monitor - Dataform Module

This module provides dataform definitions for BigQuery cost monitoring. It can be imported into other dataform projects to add cost monitoring capabilities.

## Features

- Create tables and views for BigQuery cost monitoring
- Track query costs by user, service account, dataset, and table
- Identify expensive queries and optimization opportunities
- Monitor cache hit rates and slot utilization
- Detect rebuild vs. incremental operations

## Installation

To use this module in your dataform project:

1. Copy the `module` directory to your dataform project
2. Import the module in your dataform project

```javascript
// In your dataform project's index.js
const costMonitor = require("./includes/bq-cost-monitor");

// Create all cost monitoring objects
costMonitor.createAllCostMonitoringObjects({
  schema: "analytics",
  historyDays: 30,
  costPerTerabyte: 5.0
});
```

## Usage

### Create Individual Tables and Views

You can create individual tables and views as needed:

```javascript
// Create the main cost monitoring table
const mainTable = costMonitor.createCostMonitoringTable({
  schema: "analytics",
  name: "bigquery_cost_monitoring",
  historyDays: 30,
  costPerTerabyte: 5.0
});

// Create the daily cost summary view
costMonitor.createDailyCostSummaryView({
  schema: "analytics",
  name: "daily_cost_summary",
  sourceTable: mainTable.name
});

// Create the dataset cost summary view
costMonitor.createDatasetCostSummaryView({
  schema: "analytics",
  name: "dataset_cost_summary",
  sourceTable: mainTable.name
});

// Create the service account cost summary view
costMonitor.createServiceAccountCostSummaryView({
  schema: "analytics",
  name: "service_account_cost_summary",
  sourceTable: mainTable.name
});

// Create the user dataset attribution table
costMonitor.createUserDatasetAttributionTable({
  schema: "analytics",
  name: "user_dataset_attribution",
  sourceTable: mainTable.name
});
```

### Use Query Optimization Utilities

The module also includes utilities for query optimization:

```javascript
const { queryOptimization } = require("./includes/bq-cost-monitor");

// Add optimization tips to a query
const queryWithTips = queryOptimization.optimizationTips() + myQuery;

// Add cost estimation to a query
const queryWithCost = queryOptimization.addCostEstimation(myQuery, {
  estimatedTB: 1.5,
  costPerTB: 5.0
});

// Add query tagging
const taggedQuery = queryOptimization.addQueryTagging(
  myQuery,
  "my-project",
  "data-team",
  {
    owner: "data-engineer",
    purpose: "daily reporting"
  }
);

// Add partition filter to a query
const filteredQuery = queryOptimization.addPartitionFilter(
  myQuery,
  "date",
  ">= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
);

// Make a query incremental
const incrementalQuery = queryOptimization.makeIncremental(
  myQuery,
  "last_modified_date",
  ">= '${dataform.projectConfig.vars.lastRunTime}'"
);
```

## Schema

### bigquery_cost_monitoring

Main table containing detailed cost information:

- `date`: Date of the query execution (YYYY-MM-DD)
- `project_id`: Google Cloud project ID
- `user_email`: Email of the user who executed the query
- `service_account`: Service account identifier if the query was executed by a service account
- `query_count`: Number of queries executed
- `cache_hit_count`: Number of queries that used cached results
- `error_count`: Number of queries that resulted in errors
- `total_bytes_processed`: Total bytes processed by all queries
- `total_bytes_billed`: Total bytes billed for all queries
- `estimated_cost_usd`: Estimated cost in USD based on bytes billed
- `slot_hours`: Total slot hours consumed
- `cache_hit_percentage`: Percentage of queries that used cached results
- `dataset_costs`: Array of datasets used with their associated costs

### daily_cost_summary

Daily summary of BigQuery costs across all projects:

- `date`: Date of the query execution (YYYY-MM-DD)
- `project_id`: Google Cloud project ID
- `total_queries`: Total number of queries executed
- `total_cache_hits`: Total number of queries that used cached results
- `total_errors`: Total number of queries that resulted in errors
- `total_bytes_processed`: Total bytes processed by all queries
- `total_bytes_billed`: Total bytes billed for all queries
- `total_estimated_cost_usd`: Total estimated cost in USD based on bytes billed
- `total_slot_hours`: Total slot hours consumed
- `avg_cache_hit_percentage`: Average percentage of queries that used cached results

### dataset_cost_summary

Summary of BigQuery costs by dataset:

- `date`: Date of the query execution (YYYY-MM-DD)
- `project_id`: Google Cloud project ID
- `dataset_name`: Full name of the dataset (project.dataset)
- `total_queries`: Number of queries accessing this dataset
- `total_bytes_processed`: Total bytes processed accessing this dataset
- `total_bytes_billed`: Total bytes billed for access to this dataset
- `total_cost_usd`: Total cost in USD attributed to this dataset
- `top_users`: Array of users/service accounts with highest usage of this dataset

### service_account_cost_summary

Summary of BigQuery costs by service account:

- `date`: Date of the query execution (YYYY-MM-DD)
- `project_id`: Google Cloud project ID
- `service_account`: Service account that executed the queries
- `total_queries`: Number of queries executed by the service account
- `total_bytes_processed`: Total bytes processed by the service account
- `total_bytes_billed`: Total bytes billed for the service account
- `total_cost_usd`: Total cost in USD attributed to the service account
- `top_datasets`: Array of most expensive datasets used by this service account

### user_dataset_attribution

Cross-tabulation of which users/service accounts are querying which datasets:

- `date`: Date of the query execution (YYYY-MM-DD)
- `project_id`: Google Cloud project ID
- `user`: User email or service account that executed the queries
- `is_service_account`: Whether this is a service account or a regular user
- `dataset_name`: Dataset name (project.dataset)
- `query_count`: Number of queries executed against this dataset
- `bytes_processed`: Total bytes processed
- `cost_usd`: Total cost in USD
- `top_tables`: Most expensive tables in this dataset accessed by this user
- `pct_of_user_cost`: Percentage of user's total cost that comes from this dataset
- `pct_of_dataset_cost`: Percentage of dataset's total cost that comes from this user
