/**
 * BigQuery Cost Monitor for Dataform
 * 
 * A JavaScript package that can be imported into any Dataform project to build
 * BigQuery cost monitoring tables with minimal configuration.
 */

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
   */
  constructor(options = {}) {
    this.options = {
      schema: options.schema || 'analytics',
      historyDays: options.historyDays || 30,
      costPerTerabyte: options.costPerTerabyte || 5.0
    };
    
    // Default table names
    this.tableNames = {
      costMonitoring: 'bigquery_cost_monitoring',
      dailySummary: 'daily_cost_summary',
      datasetSummary: 'dataset_cost_summary',
      serviceAccountSummary: 'service_account_cost_summary',
      userAttribution: 'user_dataset_attribution'
    };
  }
  
  /**
   * Create the main cost monitoring table
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the table in
   * @param {string} options.name - The name of the table
   * @param {number} options.historyDays - Number of days of history to include
   * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
   * @returns {Object} - The created table declaration
   */
  createCostMonitoringTable(options = {}) {
    const tableOptions = {
      schema: options.schema || this.options.schema,
      name: options.name || this.tableNames.costMonitoring,
      historyDays: options.historyDays || this.options.historyDays,
      costPerTerabyte: options.costPerTerabyte || this.options.costPerTerabyte
    };
    
    // Store the table name for reference by other views
    this.tableNames.costMonitoring = tableOptions.name;
    
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
    
    return createUserDatasetAttributionTable(this, tableOptions);
  }
  
  /**
   * Create all cost monitoring tables and views
   * @param {Object} options - Configuration options
   * @param {string} options.schema - The schema to create the objects in
   * @param {number} options.historyDays - Number of days of history to include
   * @param {number} options.costPerTerabyte - Cost per terabyte of data processed
   * @returns {Object} - Object containing all created tables and views
   */
  createAllTables(options = {}) {
    const schema = options.schema || this.options.schema;
    const historyDays = options.historyDays || this.options.historyDays;
    const costPerTerabyte = options.costPerTerabyte || this.options.costPerTerabyte;
    
    // Create the main table
    const mainTable = this.createCostMonitoringTable({
      schema,
      historyDays,
      costPerTerabyte
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
    return publish(schema, name);
  }
}

module.exports = {
  BigQueryCostMonitor,
  QueryOptimization
};
