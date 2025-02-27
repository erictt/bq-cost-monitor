/**
 * Fixed BigQuery Cost Monitor for Dataform
 * 
 * A JavaScript package that can be imported into any Dataform project to build
 * BigQuery cost monitoring tables with minimal configuration.
 * 
 * This fixed version addresses:
 * 1. Duplicate action name and canonical target errors
 * 2. Uses explicit table references without ref() function
 * 3. Support for existing datasets
 * 4. Configurable project database for INFORMATION_SCHEMA queries
 */

// Import the modified table creation modules
const createCostMonitoringTable = require('./tables/cost_monitoring');
const createDailySummaryView = require('./tables/daily_summary');
const createDatasetSummaryView = require('./tables/dataset_summary');
const createServiceAccountSummaryView = require('./tables/service_account_summary');
const createUserDatasetAttributionTable = require('./tables/user_attribution');
const QueryOptimization = require('./utils/query_optimization');

/**
 * BigQuery Cost Monitor class
 * 
 * Main class for creating and managing BigQuery cost monitoring tables and views.
 */
class BigQueryCostMonitor {
  /**
   * Create a new BigQueryCostMonitor instance
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the tables in
   * @param {number} options.historyDays - Number of days of history to include
   * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
   * @param {boolean} options.useExistingDataset - Whether to use an existing dataset (default: false)
   * @param {string} options.projectDatabase - The project database (GCP project ID) to query for INFORMATION_SCHEMA data
   */
  constructor(options = {}) {
    this.options = {
      schema: options.schema || 'analytics',
      historyDays: options.historyDays || 30,
      costPerTerabyte: options.costPerTerabyte || 5.0,
      useExistingDataset: options.useExistingDataset || false,
      projectDatabase: options.projectDatabase || null // Add project database option
    };

    // Default table names with prefixes to avoid conflicts
    this.tableNames = {
      costMonitoring: 'bq_cost_monitoring',
      dailySummary: 'bq_daily_cost_summary',
      datasetSummary: 'bq_dataset_cost_summary',
      serviceAccountSummary: 'bq_service_account_cost_summary',
      userAttribution: 'bq_user_dataset_attribution'
    };

    // Track created tables to avoid duplicates
    this.createdTables = new Set();
  }

  /**
   * Create the main cost monitoring table
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the table in
   * @param {string} options.name - The name of the table
   * @param {number} options.historyDays - Number of days of history to include
   * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
   * @param {string} options.projectDatabase - The project database (GCP project ID) to query for INFORMATION_SCHEMA data
   * @returns {Object} - The created table declaration
   */
  createCostMonitoringTable(options = {}) {
    const tableOptions = {
      schema: options.schema || this.options.schema,
      name: options.name || this.tableNames.costMonitoring,
      historyDays: options.historyDays || this.options.historyDays,
      costPerTerabyte: options.costPerTerabyte || this.options.costPerTerabyte,
      projectDatabase: options.projectDatabase || this.options.projectDatabase
    };

    // Store the table name for reference by other views
    this.tableNames.costMonitoring = tableOptions.name;

    // Check if this table has already been created
    const tableKey = `${tableOptions.schema}.${tableOptions.name}`;
    if (this.createdTables.has(tableKey)) {
      console.log(`Table ${tableKey} already created, skipping.`);
      return { name: tableOptions.name };
    }

    // Mark this table as created
    this.createdTables.add(tableKey);

    return createCostMonitoringTable(this, tableOptions);
  }

  /**
   * Create the daily cost summary view
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the view in
   * @param {string} options.name - The name of the view
   * @param {string} options.sourceTable - The name of the source table
   * @returns {Object} - The created view declaration
   */
  createDailySummaryView(options = {}) {
    const viewOptions = {
      schema: options.schema || this.options.schema,
      name: options.name || this.tableNames.dailySummary,
      sourceTable: options.sourceTable || this.tableNames.costMonitoring
    };

    // Check if this view has already been created
    const viewKey = `${viewOptions.schema}.${viewOptions.name}`;
    if (this.createdTables.has(viewKey)) {
      console.log(`View ${viewKey} already created, skipping.`);
      return { name: viewOptions.name };
    }

    // Mark this view as created
    this.createdTables.add(viewKey);

    return createDailySummaryView(this, viewOptions);
  }

  /**
   * Create the dataset cost summary view
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the view in
   * @param {string} options.name - The name of the view
   * @param {string} options.sourceTable - The name of the source table
   * @returns {Object} - The created view declaration
   */
  createDatasetSummaryView(options = {}) {
    const viewOptions = {
      schema: options.schema || this.options.schema,
      name: options.name || this.tableNames.datasetSummary,
      sourceTable: options.sourceTable || this.tableNames.costMonitoring
    };

    // Check if this view has already been created
    const viewKey = `${viewOptions.schema}.${viewOptions.name}`;
    if (this.createdTables.has(viewKey)) {
      console.log(`View ${viewKey} already created, skipping.`);
      return { name: viewOptions.name };
    }

    // Mark this view as created
    this.createdTables.add(viewKey);

    return createDatasetSummaryView(this, viewOptions);
  }

  /**
   * Create the service account cost summary view
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the view in
   * @param {string} options.name - The name of the view
   * @param {string} options.sourceTable - The name of the source table
   * @returns {Object} - The created view declaration
   */
  createServiceAccountSummaryView(options = {}) {
    const viewOptions = {
      schema: options.schema || this.options.schema,
      name: options.name || this.tableNames.serviceAccountSummary,
      sourceTable: options.sourceTable || this.tableNames.costMonitoring
    };

    // Check if this view has already been created
    const viewKey = `${viewOptions.schema}.${viewOptions.name}`;
    if (this.createdTables.has(viewKey)) {
      console.log(`View ${viewKey} already created, skipping.`);
      return { name: viewOptions.name };
    }

    // Mark this view as created
    this.createdTables.add(viewKey);

    return createServiceAccountSummaryView(this, viewOptions);
  }

  /**
   * Create the user dataset attribution table
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the table in
   * @param {string} options.name - The name of the table
   * @param {string} options.sourceTable - The name of the source table
   * @returns {Object} - The created table declaration
   */
  createUserDatasetAttributionTable(options = {}) {
    const tableOptions = {
      schema: options.schema || this.options.schema,
      name: options.name || this.tableNames.userAttribution,
      sourceTable: options.sourceTable || this.tableNames.costMonitoring
    };

    // Check if this table has already been created
    const tableKey = `${tableOptions.schema}.${tableOptions.name}`;
    if (this.createdTables.has(tableKey)) {
      console.log(`Table ${tableKey} already created, skipping.`);
      return { name: tableOptions.name };
    }

    // Mark this table as created
    this.createdTables.add(tableKey);

    return createUserDatasetAttributionTable(this, tableOptions);
  }

  /**
   * Create all cost monitoring tables and views
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the objects in
   * @param {number} options.historyDays - Number of days of history to include
   * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
   * @param {string} options.projectDatabase - The project database (GCP project ID) to query for INFORMATION_SCHEMA data
   * @returns {Object} - Object containing all created tables and views
   */
  createAllTables(options = {}) {
    const schema = options.schema || this.options.schema;
    const historyDays = options.historyDays || this.options.historyDays;
    const costPerTerabyte = options.costPerTerabyte || this.options.costPerTerabyte;
    const projectDatabase = options.projectDatabase || this.options.projectDatabase;

    // Create the main table
    const mainTable = this.createCostMonitoringTable({
      schema,
      historyDays,
      costPerTerabyte,
      projectDatabase
    });

    // Create the views
    const dailySummary = this.createDailySummaryView({ schema });
    const datasetSummary = this.createDatasetSummaryView({ schema });
    const serviceAccountSummary = this.createServiceAccountSummaryView({ schema });
    const userAttribution = this.createUserDatasetAttributionTable({ schema });

    return {
      mainTable,
      dailySummary,
      datasetSummary,
      serviceAccountSummary,
      userAttribution
    };
  }

  /**
   * Get the dataform context object
   * This is used by the table creation functions to access dataform's publish method
   * @returns {Object} - The dataform context object
   */
  publish(schema, name) {
    // In a dataform project, 'this' would be the dataform context
    // This is a pass-through to the global publish function

    // If using existing dataset, we need to handle the schema differently
    if (this.options.useExistingDataset) {
      // For existing datasets, we need to use the dataset reference syntax
      // This assumes that the dataset already exists and we're just creating tables in it

      // Generate a unique action name to avoid duplicate action name errors
      // This is especially important when creating multiple views in the same schema
      const uniqueActionName = `${schema}_${name}_${Date.now()}`;

      // Use the unique action name but target the actual schema and name
      return publish(uniqueActionName)
        .config({
          schema: schema,
          name: name
        });
    } else {
      // For new datasets, use the standard publish method
      return publish(schema, name);
    }
  }
}

module.exports = {
  BigQueryCostMonitor,
  QueryOptimization
};
