/**
 * BigQuery Cost Monitor - Dashboard Server
 * 
 * This script serves a simple web dashboard to visualize the cost monitoring data.
 */

const express = require('express');
const fs = require('fs-extra');
const path = require('path');

// Import common modules
const { logger } = require('../common/logger');
const { loadConfig } = require('../common/config-loader');

// Load configuration
const config = loadConfig();

// Create Express app
const app = express();
const port = process.env.PORT || 3000;

// Serve static files from the dashboard directory
app.use(express.static(path.join(__dirname, '../dashboard')));

// Serve files from the common directory at multiple paths to handle different import styles
app.use('/common', express.static(path.join(__dirname, '../common')));

// Log requests with detailed information
app.use((req, res, next) => {
  logger.info(`${req.method} ${req.url}`);
  logger.info(`Full URL: ${req.protocol}://${req.get('host')}${req.originalUrl}`);
  logger.info(`Headers: ${JSON.stringify(req.headers)}`);
  next();
});

// API endpoint to get the list of projects
app.get('/api/projects', (req, res) => {
  try {
    res.json(config.projects);
  } catch (error) {
    logger.error('Error loading projects:', error);
    res.status(500).json({ error: 'Failed to load projects' });
  }
});

// API endpoint to get the cost data for a specific project
app.get('/api/costs/:projectId', (req, res) => {
  try {
    const projectId = req.params.projectId;
    const outputDir = path.join(__dirname, '../../output');

    // Find the most recent cost file for this project
    const files = fs.readdirSync(outputDir)
      .filter(file => file.startsWith(`${projectId}_costs_`))
      .sort()
      .reverse();

    if (files.length === 0) {
      logger.warn(`No cost data found for project ${projectId}`);
      return res.status(404).json({ error: 'No cost data found for this project' });
    }

    const latestFile = files[0];
    const filePath = path.join(outputDir, latestFile);
    const costData = JSON.parse(fs.readFileSync(filePath, 'utf8'));

    logger.info(`Serving cost data for project ${projectId} from ${latestFile}`);
    res.json(costData);
  } catch (error) {
    logger.error(`Error loading cost data for project ${req.params.projectId}:`, error);
    res.status(500).json({ error: 'Failed to load cost data' });
  }
});

// API endpoint to get the summary data
app.get('/api/summary', (req, res) => {
  try {
    const outputDir = path.join(__dirname, '../../output');

    // Find the most recent summary file
    const files = fs.readdirSync(outputDir)
      .filter(file => file.startsWith('summary_'))
      .sort()
      .reverse();

    if (files.length === 0) {
      logger.warn('No summary data found');
      return res.status(404).json({ error: 'No summary data found' });
    }

    const latestFile = files[0];
    const filePath = path.join(outputDir, latestFile);
    const summaryData = JSON.parse(fs.readFileSync(filePath, 'utf8'));

    logger.info(`Serving summary data from ${latestFile}`);
    res.json(summaryData);
  } catch (error) {
    logger.error('Error loading summary data:', error);
    res.status(500).json({ error: 'Failed to load summary data' });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Server error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start the server
app.listen(port, () => {
  logger.info(`BigQuery Cost Monitor dashboard running at http://localhost:${port}`);
  logger.info('Press Ctrl+C to stop');
});
