/**
 * BigQuery Cost Monitor - Dashboard Server
 * 
 * This script serves a simple web dashboard to visualize the cost monitoring data.
 */

const express = require('express');
const fs = require('fs-extra');
const path = require('path');

// Create Express app
const app = express();
const port = process.env.PORT || 3000;

// Serve static files from the dashboard directory
app.use(express.static(path.join(__dirname, '../dashboard')));

// API endpoint to get the list of projects
app.get('/api/projects', (req, res) => {
  try {
    const configPath = path.join(__dirname, '../../config/projects.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    res.json(config.projects);
  } catch (error) {
    console.error('Error loading projects:', error);
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
      return res.status(404).json({ error: 'No cost data found for this project' });
    }
    
    const latestFile = files[0];
    const filePath = path.join(outputDir, latestFile);
    const costData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    
    res.json(costData);
  } catch (error) {
    console.error('Error loading cost data:', error);
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
      return res.status(404).json({ error: 'No summary data found' });
    }
    
    const latestFile = files[0];
    const filePath = path.join(outputDir, latestFile);
    const summaryData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    
    res.json(summaryData);
  } catch (error) {
    console.error('Error loading summary data:', error);
    res.status(500).json({ error: 'Failed to load summary data' });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`BigQuery Cost Monitor dashboard running at http://localhost:${port}`);
  console.log('Press Ctrl+C to stop');
});
