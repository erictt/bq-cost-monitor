/**
 * Configuration loader for BigQuery Cost Monitor
 */

const fs = require('fs-extra');
const path = require('path');
const { logger } = require('./logger');

// Default configuration values
const DEFAULT_CONFIG = {
  projects: [],
  settings: {
    historyDays: 30,
    costPerTerabyte: 5.0,
    refreshInterval: 24
  }
};

/**
 * Load configuration from a file
 * @param {string} configPath - Path to the configuration file
 * @returns {Object} - Loaded configuration with defaults applied
 */
function loadConfig(configPath) {
  const resolvedPath = configPath || process.env.CONFIG_PATH || path.join(__dirname, '../../config/projects.json');
  
  try {
    // Check if file exists
    if (!fs.existsSync(resolvedPath)) {
      logger.warn(`Configuration file not found at ${resolvedPath}, using defaults`);
      return DEFAULT_CONFIG;
    }
    
    // Read and parse the configuration file
    const config = JSON.parse(fs.readFileSync(resolvedPath, 'utf8'));
    logger.info(`Loaded configuration from ${resolvedPath}`);
    
    // Merge with defaults to ensure all required properties exist
    return {
      projects: config.projects || DEFAULT_CONFIG.projects,
      settings: {
        ...DEFAULT_CONFIG.settings,
        ...(config.settings || {})
      }
    };
  } catch (error) {
    logger.error(`Failed to load configuration: ${error.message}`);
    return DEFAULT_CONFIG;
  }
}

/**
 * Validate a configuration object
 * @param {Object} config - Configuration object to validate
 * @returns {Object} - Validation result {valid: boolean, errors: string[]}
 */
function validateConfig(config) {
  const errors = [];
  
  // Check if projects array exists
  if (!Array.isArray(config.projects)) {
    errors.push('Configuration must contain a projects array');
  } else {
    // Validate each project
    config.projects.forEach((project, index) => {
      if (!project.id) {
        errors.push(`Project at index ${index} is missing required 'id' field`);
      }
      if (!project.name) {
        errors.push(`Project at index ${index} is missing required 'name' field`);
      }
    });
  }
  
  // Check settings
  if (!config.settings) {
    errors.push('Configuration is missing settings object');
  } else {
    // Validate settings values
    if (config.settings.historyDays !== undefined && 
        (typeof config.settings.historyDays !== 'number' || config.settings.historyDays <= 0)) {
      errors.push('settings.historyDays must be a positive number');
    }
    
    if (config.settings.costPerTerabyte !== undefined && 
        (typeof config.settings.costPerTerabyte !== 'number' || config.settings.costPerTerabyte <= 0)) {
      errors.push('settings.costPerTerabyte must be a positive number');
    }
    
    if (config.settings.refreshInterval !== undefined && 
        (typeof config.settings.refreshInterval !== 'number' || config.settings.refreshInterval <= 0)) {
      errors.push('settings.refreshInterval must be a positive number');
    }
  }
  
  return {
    valid: errors.length === 0,
    errors
  };
}

/**
 * Save configuration to a file
 * @param {Object} config - Configuration to save
 * @param {string} configPath - Path to save the configuration to
 * @returns {boolean} - Whether the save was successful
 */
function saveConfig(config, configPath) {
  const resolvedPath = configPath || process.env.CONFIG_PATH || path.join(__dirname, '../../config/projects.json');
  
  try {
    // Validate configuration before saving
    const validation = validateConfig(config);
    if (!validation.valid) {
      logger.error(`Invalid configuration: ${validation.errors.join(', ')}`);
      return false;
    }
    
    // Ensure directory exists
    fs.ensureDirSync(path.dirname(resolvedPath));
    
    // Write configuration to file
    fs.writeJsonSync(resolvedPath, config, { spaces: 2 });
    logger.info(`Saved configuration to ${resolvedPath}`);
    return true;
  } catch (error) {
    logger.error(`Failed to save configuration: ${error.message}`);
    return false;
  }
}

module.exports = {
  loadConfig,
  validateConfig,
  saveConfig,
  DEFAULT_CONFIG
};
