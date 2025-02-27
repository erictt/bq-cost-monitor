/**
 * BigQuery Cost Monitor - Cloud Function Entry Point
 * 
 * This file provides a serverless entry point for the cost monitoring system.
 * It can be deployed as a Google Cloud Function and triggered by Cloud Scheduler.
 */

const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs-extra');
const path = require('path');
const moment = require('moment');
const { Storage } = require('@google-cloud/storage');

// Load monitoring logic
const { monitorProject, runCostMonitoring } = require('./run_monitor');

// GCS storage bucket for results
const BUCKET_NAME = process.env.STORAGE_BUCKET || 'bq-cost-monitor-results';

/**
 * Main entry point for the Cloud Function
 * @param {Object} req - The HTTP request object
 * @param {Object} res - The HTTP response object
 */
exports.monitorCosts = async (req, res) => {
  try {
    console.log('Starting BigQuery cost monitoring from cloud function...');
    
    // Validate request
    if (req.method !== 'POST' && req.method !== 'GET') {
      return res.status(405).send('Method not allowed');
    }
    
    // Load config
    let config;
    try {
      // Try to load from GCS if available
      const storage = new Storage();
      const bucket = storage.bucket(BUCKET_NAME);
      const file = bucket.file('config/projects.json');
      
      const [content] = await file.download();
      config = JSON.parse(content.toString());
      console.log('Loaded configuration from GCS');
    } catch (configError) {
      // Fall back to local config
      console.log('Failed to load config from GCS, using local config');
      const configPath = path.join(__dirname, '../../config/projects.json');
      config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    }
    
    console.log(`Projects to monitor: ${config.projects.length}`);
    
    // Run the monitoring for each project
    const results = [];
    for (const project of config.projects) {
      try {
        console.log(`Monitoring project: ${project.name} (${project.id})`);
        
        // Initialize BigQuery client for this project
        const bigquery = new BigQuery({
          projectId: project.id,
        });
        
        // Run cost monitoring for this project
        const result = await monitorProject(project);
        results.push(result);
        
        // Upload results to GCS
        if (!result.error) {
          try {
            const storage = new Storage();
            const bucket = storage.bucket(BUCKET_NAME);
            
            // Upload the individual project results
            const timestamp = moment().format('YYYY-MM-DD_HH-mm-ss');
            const gcsFileName = `results/${project.id}_costs_${timestamp}.json`;
            const file = bucket.file(gcsFileName);
            
            await file.save(JSON.stringify(result.data, null, 2), {
              contentType: 'application/json',
              metadata: {
                source: 'bq-cost-monitor',
                timestamp: timestamp,
                projectId: project.id
              }
            });
            
            console.log(`Cost data for ${project.name} saved to gs://${BUCKET_NAME}/${gcsFileName}`);
            result.gcsPath = `gs://${BUCKET_NAME}/${gcsFileName}`;
          } catch (uploadError) {
            console.error(`Error uploading results to GCS: ${uploadError.message}`);
            result.gcsError = uploadError.message;
          }
        }
      } catch (projectError) {
        console.error(`Error monitoring project ${project.name}:`, projectError);
        results.push({
          project: project.id,
          timestamp: moment().toISOString(),
          error: projectError.message
        });
      }
    }
    
    // Save summary
    const timestamp = moment().format('YYYY-MM-DD_HH-mm-ss');
    const summaryFileName = `summary_${timestamp}.json`;
    
    try {
      const storage = new Storage();
      const bucket = storage.bucket(BUCKET_NAME);
      const file = bucket.file(`results/${summaryFileName}`);
      
      await file.save(JSON.stringify(results, null, 2), {
        contentType: 'application/json',
        metadata: {
          source: 'bq-cost-monitor',
          timestamp: timestamp
        }
      });
      
      console.log(`Summary saved to gs://${BUCKET_NAME}/results/${summaryFileName}`);
    } catch (summaryError) {
      console.error(`Error saving summary to GCS: ${summaryError.message}`);
    }
    
    // Print summary to logs
    console.log('\nSummary:');
    results.forEach(result => {
      if (result.error) {
        console.log(`- ${result.project}: ERROR - ${result.error}`);
      } else {
        console.log(`- ${result.project}: ${result.records} records, $${result.totalCost?.toFixed(2) || 0} estimated cost`);
      }
    });
    
    // Send success response
    res.status(200).send({
      success: true,
      timestamp: moment().toISOString(),
      projects: config.projects.length,
      results: results.map(r => ({
        project: r.project,
        success: !r.error,
        records: r.records || 0,
        totalCost: r.totalCost || 0,
        error: r.error
      }))
    });
  } catch (error) {
    console.error('Error running cost monitoring:', error);
    res.status(500).send({
      success: false,
      error: error.message
    });
  }
};

// For local testing
if (require.main === module) {
  // Mock request and response objects
  const req = { method: 'GET' };
  const res = {
    status: (code) => {
      console.log(`Status: ${code}`);
      return {
        send: (data) => console.log('Response:', JSON.stringify(data, null, 2))
      };
    }
  };
  
  // Run the function
  exports.monitorCosts(req, res)
    .then(() => console.log('Done'))
    .catch(err => console.error('Error:', err));
}