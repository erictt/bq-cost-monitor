/**
 * BigQuery Cost Monitor - Main Script
 * 
 * This script runs the cost monitoring queries against the configured BigQuery projects
 * and stores the results for analysis.
 */

const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs-extra');
const path = require('path');
const moment = require('moment');
const { createLogger, format, transports } = require('winston');

// Setup logging
const logger = createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: format.combine(
    format.timestamp(),
    format.json()
  ),
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.simple()
      )
    }),
    new transports.File({ 
      filename: path.join(__dirname, '../../logs/cost-monitor.log'),
      maxsize: 5242880, // 5MB
      maxFiles: 5,
    })
  ]
});

// Environment variables with defaults
const DEFAULT_LOCATION = process.env.BQ_LOCATION || 'US';
const DEFAULT_HISTORY_DAYS = parseInt(process.env.HISTORY_DAYS || '30', 10);
const DEFAULT_COST_PER_TB = parseFloat(process.env.COST_PER_TB || '5.0');

// Load configuration
let config;
try {
  const configPath = process.env.CONFIG_PATH || path.join(__dirname, '../../config/projects.json');
  config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  logger.info(`Loaded configuration from ${configPath}`);
} catch (error) {
  logger.error(`Failed to load configuration: ${error.message}`);
  config = { 
    projects: [],
    settings: {
      historyDays: DEFAULT_HISTORY_DAYS,
      costPerTerabyte: DEFAULT_COST_PER_TB,
      refreshInterval: 24
    }
  };
}

// Load SQL queries
let usageQuery, costQuery;
try {
  const usageQueryPath = path.join(__dirname, '../queries/usage_query.sql');
  const costQueryPath = path.join(__dirname, '../queries/cost_query.sql');
  usageQuery = fs.readFileSync(usageQueryPath, 'utf8');
  costQuery = fs.readFileSync(costQueryPath, 'utf8');
  logger.info('SQL queries loaded successfully');
} catch (error) {
  logger.error(`Failed to load SQL queries: ${error.message}`);
  process.exit(1);
}

// Ensure output directory exists
const outputDir = path.join(__dirname, '../../output');
fs.ensureDirSync(outputDir);

// Ensure logs directory exists
fs.ensureDirSync(path.join(__dirname, '../../logs'));

/**
 * Run the cost monitoring for a specific project
 * @param {Object} project - Project configuration
 * @param {Object} [options] - Options for the monitoring
 * @param {number} [options.historyDays] - Number of days of history to query
 * @param {number} [options.costPerTerabyte] - Cost per terabyte of data processed
 * @param {string} [options.location] - BigQuery location
 * @returns {Promise<Object>} - Results of the cost monitoring
 */
async function monitorProject(project, options = {}) {
  const startTime = Date.now();
  const loggingMeta = { projectId: project.id, projectName: project.name };
  
  logger.info(`Monitoring project: ${project.name} (${project.id})`, loggingMeta);
  
  // Use options with fallbacks to config and then defaults
  const historyDays = options.historyDays || 
                      (config.settings && config.settings.historyDays) || 
                      DEFAULT_HISTORY_DAYS;
                     
  const costPerTerabyte = options.costPerTerabyte || 
                         (config.settings && config.settings.costPerTerabyte) || 
                         DEFAULT_COST_PER_TB;
                        
  const location = options.location || 
                  (project.location) || 
                  DEFAULT_LOCATION;
  
  try {
    // Initialize BigQuery client for this project
    const bigquery = new BigQuery({
      projectId: project.id,
    });
    
    // Run the cost query
    logger.info(`Running cost query for ${project.id}...`, {
      ...loggingMeta, 
      historyDays, 
      costPerTerabyte,
      location
    });
    
    const [costRows] = await bigquery.query({
      query: costQuery,
      params: {
        history_days: historyDays,
        cost_per_terabyte: costPerTerabyte
      },
      location: location,
      timeout: 180000 // 3 minute timeout
    });
    
    logger.info(`Retrieved ${costRows.length} cost records for ${project.name}`, loggingMeta);
    
    // Calculate totals and statistics
    const totalCost = costRows.reduce((sum, row) => sum + (row.estimated_cost_usd || 0), 0);
    const totalBytes = costRows.reduce((sum, row) => sum + (row.total_bytes_processed || 0), 0);
    const totalQueries = costRows.reduce((sum, row) => sum + (row.query_count || 0), 0);
    
    // Save results to output directory
    const timestamp = moment().format('YYYY-MM-DD_HH-mm-ss');
    const outputPath = path.join(outputDir, `${project.id}_costs_${timestamp}.json`);
    await fs.writeJson(outputPath, costRows, { spaces: 2 });
    
    logger.info(`Cost data for ${project.name} saved to ${outputPath}`, loggingMeta);
    
    // Calculate elapsed time
    const elapsedSeconds = (Date.now() - startTime) / 1000;
    
    // Return the results with detailed information
    return {
      project: project.id,
      projectName: project.name,
      timestamp: moment().toISOString(),
      records: costRows.length,
      totalCost: totalCost,
      totalBytesProcessed: totalBytes,
      totalQueries: totalQueries,
      elapsedTime: elapsedSeconds,
      outputPath,
      data: costRows, // Include the actual data for potential direct usage
      params: {
        historyDays,
        costPerTerabyte,
        location
      }
    };
  } catch (error) {
    logger.error(`Error monitoring project ${project.name}: ${error.message}`, {
      ...loggingMeta,
      error: error.message,
      stack: error.stack
    });
    
    return {
      project: project.id,
      projectName: project.name,
      timestamp: moment().toISOString(),
      error: error.message,
      errorDetails: error.stack,
      params: {
        historyDays,
        costPerTerabyte,
        location
      }
    };
  }
}

/**
 * Main function to run the cost monitoring for all projects
 * @param {Object} [options] - Options for the monitoring
 * @returns {Promise<Array>} - Array of results for each project
 */
async function runCostMonitoring(options = {}) {
  const startTime = Date.now();
  logger.info('Starting BigQuery cost monitoring...');
  logger.info(`Projects to monitor: ${config.projects.length}`);
  
  const results = [];
  
  for (const project of config.projects) {
    // Skip projects marked as disabled if they exist
    if (project.disabled) {
      logger.info(`Skipping disabled project: ${project.name} (${project.id})`);
      continue;
    }
    
    // Monitor each project with provided options
    const result = await monitorProject(project, options);
    results.push(result);
  }
  
  // Save summary of all results
  const summaryPath = path.join(outputDir, `summary_${moment().format('YYYY-MM-DD')}.json`);
  await fs.writeJson(summaryPath, results, { spaces: 2 });
  
  // Calculate elapsed time
  const elapsedSeconds = (Date.now() - startTime) / 1000;
  logger.info(`Cost monitoring completed in ${elapsedSeconds.toFixed(2)} seconds. Summary saved to ${summaryPath}`);
  
  // Print summary to console
  logger.info('Summary:');
  results.forEach(result => {
    if (result.error) {
      logger.error(`- ${result.project}: ERROR - ${result.error}`);
    } else {
      logger.info(`- ${result.project}: ${result.records} records, $${result.totalCost.toFixed(2)} estimated cost`);
    }
  });
  
  return results;
}

// If this script is run directly (not imported)
if (require.main === module) {
  // Run the cost monitoring
  runCostMonitoring().catch(error => {
    logger.error('Error running cost monitoring:', error);
    process.exit(1);
  });
}

// Export functions for use in other modules
module.exports = {
  monitorProject,
  runCostMonitoring,
  logger
};
