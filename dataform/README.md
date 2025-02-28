# BigQuery Cost Monitor for Dataform

## Installation

You can use this package in your Dataform projects with following two ways:

1. Copy the entire `dataform` directory to your project
2. Import the package in your Dataform project

## Usage with Existing Datasets

```javascript
// Import the modified BigQueryCostMonitor class
// const { BigQueryCostMonitor } = require("./path/to/modified_index");
const { BigQueryCostMonitor } = require("bq-cost-monitor");

// Initialize with an existing dataset
const costMonitor = new BigQueryCostMonitor({
  schema: "data_governance",  // Your existing dataset
  historyDays: 30,
  costPerTerabyte: 5.0,
  useExistingDataset: true,   // Enable existing dataset support
  projectDatabase: "my-gcp-project"  // Specify the project database (GCP project ID)
});

// Create tables with unique names
const mainTable = costMonitor.createCostMonitoringTable({
  name: "bq_cost_monitoring_table"  // Use a unique name
});

// Create views with unique names
costMonitor.createDailySummaryView({
  name: "bq_daily_cost_summary",
  sourceTable: mainTable.name
});
```

## Configuration Options

The `BigQueryCostMonitor` constructor accepts the following options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | string | 'analytics' | The schema (dataset) to create the tables in |
| `historyDays` | number | 30 | Number of days of history to include |
| `costPerTerabyte` | number | 5.0 | Cost per terabyte of data processed |
| `useExistingDataset` | boolean | false | Whether to use an existing dataset |
| `projectDatabase` | string | null | The GCP project ID to query for INFORMATION_SCHEMA data |

## Troubleshooting

If you still encounter issues with duplicate actions or canonical targets:

1. **Use unique table and view names**: Make sure each table and view has a unique name across your entire Dataform project. Consider using a prefix like `bq_` for all cost monitoring tables.

2. **Check your Dataform configuration**: Your Dataform project might have default schema settings that are causing conflicts. Check your `dataform.json` file for any default schema configurations.

3. **Look for existing tables**: Check if there are any existing tables in your BigQuery project with the same names as the ones you're trying to create.

4. **Try a different schema**: If possible, use a completely different schema name that doesn't exist in your project.

5. **Debug with console logs**: The modified package includes console logs that can help identify which tables are being created and which ones are being skipped due to duplicates.

## Common Errors and Solutions

### "Duplicate action name detected"

This error occurs when Dataform detects multiple actions (tables, views, etc.) with the same name. To fix:

- Use unique names for all tables and views
- Make sure you're not calling the same creation method multiple times
- Check if the table or view already exists in your Dataform project

This package now automatically generates unique action names when using existing datasets to avoid this error.

### "Duplicate canonical target detected"

This error occurs when Dataform detects multiple objects targeting the same BigQuery table or view. To fix:

- Use the `useExistingDataset: true` option
- Use unique schema and table name combinations
- Check your Dataform project for any default schema settings

The latest version (0.0.2+) includes a fix that uses unique action names with the correct schema and table name configuration to avoid this error.

## Performance Optimization

The package now includes filters to reduce the amount of data processed in the query:

- Excludes queries with zero bytes processed
- Excludes small queries (less than 1MB) that don't contribute significantly to costs

These optimizations can significantly reduce the amount of data processed (from 600GB+ to much less) while still providing accurate cost monitoring information.

## Example Files

- `examples/cost-monitor.js`: Shows how to use the package with existing datasets

## Package Structure

```
dataform/
├── examples/            # Example usage
│   └── cost-monitor.js  # Example implementation
├── src/                 # Package source code
│   ├── index.js         # Package entry point
│   ├── tables/          # Table definitions
│   │   ├── cost_monitoring.js      # Main cost table
│   │   ├── daily_summary.js        # Daily summary view
│   │   ├── dataset_summary.js      # Dataset summary view
│   │   ├── service_account_summary.js # Service account view
│   │   └── user_attribution.js     # User attribution table
│   └── utils/           # Utilities
│       └── query_optimization.js   # Query optimization
├── package.json         # Package manifest
└── README.md            # This file
```
