/**
 * Centralized logging configuration for BigQuery Cost Monitor
 */

const { createLogger, format, transports } = require('winston');
const path = require('path');
const fs = require('fs-extra');

// Ensure logs directory exists
const logsDir = path.join(__dirname, '../../logs');
fs.ensureDirSync(logsDir);

/**
 * Create a configured logger instance
 * @param {Object} options - Logger configuration options
 * @param {string} options.name - Logger name/identifier
 * @param {string} options.level - Logging level (debug, info, warn, error)
 * @param {boolean} options.console - Whether to log to console
 * @param {boolean} options.file - Whether to log to file
 * @param {string} options.filename - Log filename (if file logging enabled)
 * @returns {Object} - Configured Winston logger
 */
function createAppLogger(options = {}) {
  const {
    name = 'bq-cost-monitor',
    level = process.env.LOG_LEVEL || 'info',
    console = true,
    file = true,
    filename = 'cost-monitor.log'
  } = options;

  // Define transport array
  const logTransports = [];

  // Add console transport if enabled
  if (console) {
    logTransports.push(
      new transports.Console({
        format: format.combine(
          format.colorize(),
          format.simple()
        )
      })
    );
  }

  // Add file transport if enabled
  if (file) {
    logTransports.push(
      new transports.File({
        filename: path.join(logsDir, filename),
        maxsize: 5242880, // 5MB
        maxFiles: 5,
      })
    );
  }

  // Create and return the logger
  return createLogger({
    level,
    format: format.combine(
      format.timestamp(),
      format.json()
    ),
    defaultMeta: { service: name },
    transports: logTransports
  });
}

// Create default logger
const defaultLogger = createAppLogger();

module.exports = {
  createAppLogger,
  logger: defaultLogger
};
