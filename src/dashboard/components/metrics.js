/**
 * Metrics components for BigQuery Cost Monitor dashboard
 */

console.log('metrics.js loaded');

import { formatBytes, formatCurrency } from '../formatters.js';

/**
 * Update the summary metrics with the current data
 * @param {Array} data - The data to use for the summary
 * @param {Object} elements - Object containing DOM elements for metrics
 * @param {HTMLElement} elements.totalCostElement - Element for total cost
 * @param {HTMLElement} elements.dataProcessedElement - Element for data processed
 * @param {HTMLElement} elements.queryCountElement - Element for query count
 */
function updateSummaryMetrics(data, elements) {
  const { totalCostElement, dataProcessedElement, queryCountElement } = elements;
  
  // Calculate total cost
  const totalCost = data.reduce((sum, item) => sum + (item.estimated_cost_usd || 0), 0);
  totalCostElement.textContent = formatCurrency(totalCost);
  
  // Calculate total bytes processed
  const totalBytes = data.reduce((sum, item) => sum + (item.total_bytes_processed || 0), 0);
  const totalTerabytes = totalBytes / Math.pow(1024, 4);
  dataProcessedElement.textContent = `${totalTerabytes.toFixed(2)} TB`;
  
  // Calculate total query count
  const totalQueries = data.reduce((sum, item) => sum + (item.query_count || 0), 0);
  queryCountElement.textContent = totalQueries.toLocaleString();
}

/**
 * Generate sample data for demonstration purposes
 * @returns {Array} - Sample data
 */
function generateSampleData() {
  const data = [];
  
  // Define realistic users and service accounts
  const entities = [
    { type: 'user', id: 'finance-analyst@company.com' },
    { type: 'user', id: 'data-scientist@company.com' },
    { type: 'sa', id: 'dataform-prod@company-123456.iam.gserviceaccount.com' },
    { type: 'sa', id: 'dbt-runner@company-123456.iam.gserviceaccount.com' }
  ];
  
  // Define realistic datasets with usage patterns
  const datasetProfiles = [
    { 
      name: 'company-data.billing_core', 
      avgBytes: 1.5e12, // 1.5 TB
      stdDevBytes: 0.3e12,
      users: ['finance-analyst@company.com', 'dataform-prod@company-123456.iam.gserviceaccount.com']
    },
    { 
      name: 'company-data.marketing_events', 
      avgBytes: 5e11, // 500 GB
      stdDevBytes: 1e11,
      users: ['data-scientist@company.com', 'dbt-runner@company-123456.iam.gserviceaccount.com']
    },
    { 
      name: 'external-analytics.metrics', 
      avgBytes: 2e12, // 2 TB
      stdDevBytes: 0.5e12,
      users: ['dataform-prod@company-123456.iam.gserviceaccount.com']
    },
    { 
      name: 'user-data.activity_logs', 
      avgBytes: 3e12, // 3 TB
      stdDevBytes: 0.8e12,
      users: ['data-scientist@company.com', 'dbt-runner@company-123456.iam.gserviceaccount.com']
    },
    { 
      name: 'company-data.financial_reports', 
      avgBytes: 8e11, // 800 GB
      stdDevBytes: 2e11,
      users: ['finance-analyst@company.com']
    }
  ];
  
  // Days with reduced weekend activity
  const weekendReduction = 0.3; // 70% reduction on weekends
  
  // Generate data for the last 30 days
  for (let i = 0; i < 30; i++) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    const dateString = date.toISOString().split('T')[0];
    
    // Check if it's a weekend
    const isWeekend = date.getDay() === 0 || date.getDay() === 6;
    const dayFactor = isWeekend ? weekendReduction : 1.0;
    
    // Create entries for each entity
    entities.forEach(entity => {
      let totalBytes = 0;
      let totalCost = 0;
      const isServiceAccount = entity.type === 'sa';
      
      // Determine which datasets this entity uses
      const relevantDatasets = datasetProfiles.filter(ds => 
        ds.users.includes(entity.id)
      );
      
      if (relevantDatasets.length === 0) {
        return; // Skip if no datasets for this entity
      }
      
      // Generate dataset costs for this entity
      const datasetCosts = relevantDatasets.map(dataset => {
        // Calculate bytes with normal distribution and day factor
        const normalRandom = () => {
          let u = 0, v = 0;
          while (u === 0) u = Math.random();
          while (v === 0) v = Math.random();
          return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
        };
        
        // Calculate bytes with variation based on normal distribution
        const baseBytesProcessed = Math.max(
          dataset.avgBytes + normalRandom() * dataset.stdDevBytes,
          dataset.avgBytes * 0.1
        );
        
        // Apply weekend factor and some randomness
        const bytesProcessed = Math.round(
          baseBytesProcessed * dayFactor * (0.8 + Math.random() * 0.4)
        );
        
        // Round up to the nearest 1 MB for billing
        const bytesBilled = Math.ceil(bytesProcessed / 1048576) * 1048576;
        
        // Calculate cost ($5 per TB)
        const cost = (bytesBilled / Math.pow(1024, 4)) * 5;
        
        totalBytes += bytesProcessed;
        totalCost += cost;
        
        return {
          dataset: dataset.name,
          bytes_processed: bytesProcessed,
          bytes_billed: bytesBilled,
          dataset_cost_usd: cost
        };
      });
      
      // Calculate query count based on bytes and entity type
      // Service accounts tend to run larger but fewer queries
      const bytesPerQuery = isServiceAccount ? 2e11 : 5e10; // 200GB per SA query, 50GB per user query
      const baseQueryCount = Math.max(5, Math.round(totalBytes / bytesPerQuery));
      const queryCount = Math.round(baseQueryCount * (0.8 + Math.random() * 0.4));
      
      // Cache hit rate varies by entity type
      // Service accounts often run more repeated queries with higher cache hit rates
      const cacheHitRate = isServiceAccount ? 0.3 + Math.random() * 0.4 : 0.1 + Math.random() * 0.2;
      const cacheHitCount = Math.round(queryCount * cacheHitRate);
      
      // Error count - relatively low
      const errorRate = 0.02 + Math.random() * 0.03; // 2-5% error rate
      const errorCount = Math.round(queryCount * errorRate);
      
      // Slot utilization - higher for service accounts typically
      const slotMultiplier = isServiceAccount ? 1.5 : 0.8;
      const slotHours = (totalBytes / 1e12) * slotMultiplier * (0.8 + Math.random() * 0.4);
      
      // Create the data entry
      data.push({
        date: dateString,
        project_id: 'company-123456',
        user_email: isServiceAccount ? null : entity.id,
        service_account: isServiceAccount ? entity.id : null,
        query_count: queryCount,
        cache_hit_count: cacheHitCount,
        error_count: errorCount,
        total_bytes_processed: totalBytes,
        total_bytes_billed: totalBytes, // Simplification
        estimated_cost_usd: totalCost,
        slot_hours: slotHours,
        cache_hit_percentage: (cacheHitCount / queryCount) * 100,
        dataset_costs: datasetCosts
      });
    });
  }
  
  return data;
}

/**
 * Show loading indicators
 * @param {NodeList|Array} elements - Elements to show loading indicators on
 */
function showLoading(elements) {
  elements.forEach(element => {
    if (!element.querySelector('.loading')) {
      const loading = document.createElement('div');
      loading.classList.add('loading');
      element.appendChild(loading);
    }
  });
}

/**
 * Hide loading indicators
 */
function hideLoading() {
  document.querySelectorAll('.loading').forEach(element => {
    element.remove();
  });
}

/**
 * Show an empty state when no data is available
 * @param {Object} elements - Object containing DOM elements
 * @param {HTMLElement} elements.totalCostElement - Element for total cost
 * @param {HTMLElement} elements.dataProcessedElement - Element for data processed
 * @param {HTMLElement} elements.queryCountElement - Element for query count
 * @param {HTMLElement} elements.queriesTableElement - Element for queries table
 * @param {HTMLElement} elements.datasetTableElement - Element for dataset table
 * @param {string} [message] - Optional custom message to display
 * @param {Function} resetCharts - Function to reset charts
 * @param {Function} showSampleData - Function to show sample data
 */
function showEmptyState(elements, message, resetCharts, showSampleData) {
  hideLoading();
  
  const { 
    totalCostElement, 
    dataProcessedElement, 
    queryCountElement, 
    queriesTableElement, 
    datasetTableElement 
  } = elements;
  
  // Update summary metrics with zeros
  totalCostElement.textContent = '$0.00';
  dataProcessedElement.textContent = '0 TB';
  queryCountElement.textContent = '0';
  
  // Clear the tables
  queriesTableElement.innerHTML = '';
  datasetTableElement.innerHTML = '';
  
  // Default message if none provided
  const defaultMessage = 'No cost data available for this project yet.';
  const instructionMessage = 'Run the cost monitoring script to collect data.';
  const displayMessage = message || defaultMessage;
  
  // Add empty state message to the queries table
  const emptyRow = document.createElement('tr');
  emptyRow.innerHTML = `
    <td colspan="6">
      <div class="empty-state">
        <p>${displayMessage}</p>
        <p>${instructionMessage}</p>
        <p><button id="showSampleBtn" class="btn btn-sm btn-outline-secondary">Show Sample Data</button></p>
      </div>
    </td>
  `;
  queriesTableElement.appendChild(emptyRow);
  
  // Add empty state message to the dataset table
  const emptyDatasetRow = document.createElement('tr');
  emptyDatasetRow.innerHTML = `
    <td colspan="4">
      <div class="empty-state">
        <p>No dataset cost information available</p>
      </div>
    </td>
  `;
  datasetTableElement.appendChild(emptyDatasetRow);
  
  // Reset charts
  resetCharts();
  
  // Add event listener to the sample data button
  setTimeout(() => {
    const sampleButton = document.getElementById('showSampleBtn');
    if (sampleButton) {
      sampleButton.addEventListener('click', showSampleData);
    }
  }, 0);
}

/**
 * Show an error message
 * @param {string} message - The error message to display
 */
function showError(message) {
  console.error(message);
  alert(message);
}

export {
  updateSummaryMetrics,
  generateSampleData,
  showLoading,
  hideLoading,
  showEmptyState,
  showError
};
