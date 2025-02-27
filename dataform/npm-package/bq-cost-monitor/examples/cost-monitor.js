/**
 * Fixed example of how to use the bq-cost-monitor package in a Dataform project
 * 
 * This example demonstrates how to use the fixed version of the BigQueryCostMonitor
 * package that addresses the duplicate action name and syntax errors, optimizes
 * query performance, and fixes issues with duplicate canonical targets.
 */

// Import the BigQueryCostMonitor class from the fixed package
// Note: In your actual code, you would still use require("bq-cost-monitor")
// This is just for demonstration purposes
const { BigQueryCostMonitor } = require("bq-cost-monitoring");

// Example: Using with an existing dataset (data_governance)
// Initialize with the useExistingDataset option set to true
const costMonitor = new BigQueryCostMonitor({
  schema: "data_governance",  // Your existing dataset
  historyDays: 30,
  costPerTerabyte: 5.0,
  useExistingDataset: true,   // This is the key option to use existing datasets
  projectDatabase: "my-gcp-project"  // Specify the project database (GCP project ID)
});

// Create the main cost monitoring table with a unique name
// The fixed package uses prefixed table names by default to avoid conflicts
const mainTable = costMonitor.createCostMonitoringTable();

// Create the daily cost summary view
costMonitor.createDailySummaryView({
  sourceTable: mainTable.name
});

/**
 * IMPLEMENTATION NOTES:
 * 
 * The fixed version of the package includes the following improvements:
 * 
 * 1. Uses explicit table references without ref() function
 * 2. Added tracking of created tables/views to avoid duplicates
 * 3. Added support for existing datasets with the useExistingDataset option
 * 4. Uses prefixed table names by default (bq_*) to avoid conflicts
 * 5. Added support for configurable project database with the projectDatabase option
 * 6. Optimized query performance to reduce data processed (from 600GB+ to much less)
 * 7. Fixed duplicate action name and canonical target errors with unique action names
 * 
 * If you need to specify custom table names, you can still do so:
 * 
 * const mainTable = costMonitor.createCostMonitoringTable({
 *   name: "my_custom_table_name"
 * });
 * 
 * costMonitor.createDailySummaryView({
 *   name: "my_custom_view_name",
 *   sourceTable: mainTable.name
 * });
 * 
 * IMPORTANT: When using existing datasets, the package now automatically generates
 * unique action names to avoid duplicate action name and canonical target errors.
 * This means you can safely create tables and views with the same names in different
 * Dataform projects without conflicts.
 */
