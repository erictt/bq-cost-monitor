/**
 * BigQuery Cost Monitor - Dashboard Application
 */

// Global variables
let currentProject = null;
let costData = [];
let costTrendChart = null;
let userCostChart = null;
let cacheHitChart = null;
let activePeriod = 14; // Default to 14 days

/**
 * Format bytes to human-readable format
 * @param {number} bytes - The number of bytes to format
 * @returns {string} - Formatted string with appropriate unit
 */
function formatBytes(bytes) {
  if (bytes === 0 || isNaN(bytes)) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[Math.min(i, sizes.length - 1)];
}

/**
 * Format currency value
 * @param {number} value - The value to format as currency
 * @returns {string} - Formatted currency string
 */
function formatCurrency(value) {
  if (typeof value !== 'number' || isNaN(value)) return '$0.00';
  // Use toLocaleString for proper currency formatting with commas
  return '$' + value.toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
}

/**
 * Format percentage value
 * @param {number} value - The value to calculate percentage from
 * @param {number} total - The total value
 * @returns {string} - Formatted percentage string
 */
function formatPercentage(value, total) {
  if (typeof value !== 'number' || typeof total !== 'number' || 
      isNaN(value) || isNaN(total) || total === 0) {
    return '0.0%';
  }
  
  const percentage = (value / total) * 100;
  // Cap at 100% for display purposes if it somehow exceeds 100%
  const cappedPercentage = Math.min(percentage, 100);
  return cappedPercentage.toFixed(1) + '%';
}

// DOM elements
const projectDropdown = document.getElementById('projectDropdown');
const projectList = document.getElementById('projectList');
const totalCostElement = document.getElementById('totalCost');
const dataProcessedElement = document.getElementById('dataProcessed');
const queryCountElement = document.getElementById('queryCount');
const queriesTableElement = document.getElementById('queriesTable');
const datasetTableElement = document.getElementById('datasetTable');
const periodButtons = document.querySelectorAll('[data-period]');

// Initialize the dashboard
document.addEventListener('DOMContentLoaded', () => {
  // Load projects
  loadProjects();
  
  // Set up event listeners
  setupEventListeners();
});

/**
 * Load the list of projects from the API
 */
async function loadProjects() {
  try {
    const response = await fetch('/api/projects');
    if (!response.ok) {
      throw new Error('Failed to load projects');
    }
    
    const projects = await response.json();
    
    // Populate the project dropdown
    projectList.innerHTML = '';
    projects.forEach(project => {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.classList.add('dropdown-item');
      a.textContent = project.name;
      a.dataset.projectId = project.id;
      a.href = '#';
      a.addEventListener('click', () => selectProject(project));
      li.appendChild(a);
      projectList.appendChild(li);
    });
    
    // Select the first project by default
    if (projects.length > 0) {
      selectProject(projects[0]);
    } else {
      showEmptyState();
    }
  } catch (error) {
    console.error('Error loading projects:', error);
    showError('Failed to load projects. Please try again later.');
  }
}

/**
 * Select a project and load its data
 * @param {Object} project - The project to select
 */
function selectProject(project) {
  currentProject = project;
  projectDropdown.textContent = project.name;
  
  // Load the cost data for this project
  loadCostData(project.id);
}

/**
 * Load cost data for a specific project
 * @param {string} projectId - The ID of the project
 * @param {boolean} useSampleData - Whether to use sample data if real data is unavailable 
 */
async function loadCostData(projectId, useSampleData = false) {
  try {
    showLoading();
    
    // Try to load from the API
    const response = await fetch(`/api/costs/${projectId}`);
    
    // If no data is available yet
    if (!response.ok) {
      console.warn(`No cost data available for project ${projectId}`);
      
      if (useSampleData) {
        // Only show sample data if explicitly requested
        showSampleData();
      } else {
        // Otherwise show the empty state
        showEmptyState("No cost data available for this project");
        hideLoading();
      }
      return;
    }
    
    // Parse the real data
    costData = await response.json();
    
    // Update the dashboard with the loaded data
    updateDashboard();
  } catch (error) {
    console.error('Error loading cost data:', error);
    
    if (useSampleData) {
      // Only show sample data if explicitly requested
      showSampleData();
    } else {
      // Otherwise show the empty state with error
      showEmptyState(`Error loading cost data: ${error.message}`);
      hideLoading();
    }
  }
}

/**
 * Update the dashboard with the current data
 */
function updateDashboard() {
  if (!costData || costData.length === 0) {
    showEmptyState();
    return;
  }
  
  // Filter data based on the active period
  const filteredData = filterDataByPeriod(costData, activePeriod);
  
  // Update summary metrics
  updateSummaryMetrics(filteredData);
  
  // Update charts
  updateCharts(filteredData);
  
  // Update the query table
  updateTable(filteredData);
  
  // Update dataset table
  updateDatasetTable(filteredData);
  
  // Hide loading indicators
  hideLoading();
}

/**
 * Filter data by the selected time period
 * @param {Array} data - The data to filter
 * @param {number} days - The number of days to include
 * @returns {Array} - The filtered data
 */
function filterDataByPeriod(data, days) {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - days);
  
  return data.filter(item => {
    const itemDate = new Date(item.date);
    return itemDate >= cutoffDate;
  });
}

/**
 * Update the summary metrics with the current data
 * @param {Array} data - The data to use for the summary
 */
function updateSummaryMetrics(data) {
  // Calculate total cost
  const totalCost = data.reduce((sum, item) => sum + (item.estimated_cost_usd || 0), 0);
  totalCostElement.textContent = `$${totalCost.toFixed(2)}`;
  
  // Calculate total bytes processed
  const totalBytes = data.reduce((sum, item) => sum + (item.total_bytes_processed || 0), 0);
  const totalTerabytes = totalBytes / Math.pow(1024, 4);
  dataProcessedElement.textContent = `${totalTerabytes.toFixed(2)} TB`;
  
  // Calculate total query count
  const totalQueries = data.reduce((sum, item) => sum + (item.query_count || 0), 0);
  queryCountElement.textContent = totalQueries.toLocaleString();
}

/**
 * Update the charts with the current data
 * @param {Array} data - The data to use for the charts
 */
function updateCharts(data) {
  // Prepare data for charts
  const dates = [...new Set(data.map(item => item.date))].sort();
  
  // Cost trend chart
  updateCostTrendChart(dates, data);
  
  // User cost chart
  updateUserCostChart(data);
  
  // Cache hit chart
  updateCacheHitChart(data);
}

/**
 * Update the cost trend chart
 * @param {Array} dates - The dates to include in the chart
 * @param {Array} data - The data to use for the chart
 */
function updateCostTrendChart(dates, data) {
  // Aggregate cost by date
  const costByDate = {};
  dates.forEach(date => {
    costByDate[date] = data
      .filter(item => item.date === date)
      .reduce((sum, item) => sum + (item.estimated_cost_usd || 0), 0);
  });
  
  const chartData = {
    labels: dates,
    datasets: [{
      label: 'Daily Cost (USD)',
      data: dates.map(date => costByDate[date]),
      backgroundColor: 'rgba(13, 110, 253, 0.2)',
      borderColor: 'rgba(13, 110, 253, 1)',
      borderWidth: 2,
      tension: 0.1,
      fill: true
    }]
  };
  
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: 'Cost (USD)'
        }
      },
      x: {
        title: {
          display: true,
          text: 'Date'
        }
      }
    }
  };
  
  // Create or update the chart
  const ctx = document.getElementById('costTrendChart').getContext('2d');
  
  if (costTrendChart) {
    costTrendChart.data = chartData;
    costTrendChart.options = chartOptions;
    costTrendChart.update();
  } else {
    costTrendChart = new Chart(ctx, {
      type: 'line',
      data: chartData,
      options: chartOptions
    });
  }
}

/**
 * Update the user cost chart
 * @param {Array} data - The data to use for the chart
 */
function updateUserCostChart(data) {
  // Aggregate cost by user and service account
  const costByEntity = {};
  
  data.forEach(item => {
    // If service account exists, use it; otherwise use user_email
    const entityId = item.service_account || item.user_email || 'Unknown';
    
    if (!costByEntity[entityId]) {
      costByEntity[entityId] = {
        cost: 0,
        isServiceAccount: !!item.service_account,
        type: item.service_account ? 'Service Account' : 'User'
      };
    }
    
    costByEntity[entityId].cost += (item.estimated_cost_usd || 0);
  });
  
  // Sort entities by cost and take top 8
  const topEntities = Object.entries(costByEntity)
    .sort((a, b) => b[1].cost - a[1].cost)
    .slice(0, 8);
  
  // Format display names for readability
  const displayNames = topEntities.map(([entity, info]) => {
    if (info.isServiceAccount) {
      // For service accounts, format to show just key part
      return entity.split('@')[0].replace('service-', 'SA:');
    } else {
      // For users, just show the username part
      return entity.split('@')[0];
    }
  });
  
  // Set up separate datasets for users and service accounts
  const userCosts = topEntities
    .filter(([_, info]) => !info.isServiceAccount)
    .map(([_, info]) => info.cost);
  
  const saCosts = topEntities
    .filter(([_, info]) => info.isServiceAccount)
    .map(([_, info]) => info.cost);
  
  const chartData = {
    labels: displayNames,
    datasets: [{
      label: 'User',
      data: topEntities.map(([_, info]) => info.isServiceAccount ? 0 : info.cost),
      backgroundColor: 'rgba(13, 110, 253, 0.7)',
      borderWidth: 1
    },
    {
      label: 'Service Account',
      data: topEntities.map(([_, info]) => info.isServiceAccount ? info.cost : 0),
      backgroundColor: 'rgba(220, 53, 69, 0.7)',
      borderWidth: 1
    }]
  };
  
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top'
      },
      tooltip: {
        callbacks: {
          // Custom tooltip to show the actual cost values
          label: function(context) {
            const label = context.dataset.label || '';
            const value = context.raw || 0;
            return value > 0 ? `${label}: $${value.toFixed(2)}` : null;
          }
        }
      }
    },
    scales: {
      x: {
        stacked: true,
        title: {
          display: true,
          text: 'User / Service Account'
        }
      },
      y: {
        stacked: true,
        title: {
          display: true,
          text: 'Cost (USD)'
        },
        beginAtZero: true
      }
    }
  };
  
  // Create or update the chart
  const ctx = document.getElementById('userCostChart').getContext('2d');
  
  if (userCostChart) {
    userCostChart.data = chartData;
    userCostChart.options = chartOptions;
    userCostChart.update();
  } else {
    userCostChart = new Chart(ctx, {
      type: 'bar', // Changed to bar for better visualization of stacked data
      data: chartData,
      options: chartOptions
    });
  }
}

/**
 * Update the cache hit chart
 * @param {Array} data - The data to use for the chart
 */
function updateCacheHitChart(data) {
  // Calculate overall cache hit percentage
  const totalQueries = data.reduce((sum, item) => sum + (item.query_count || 0), 0);
  const cacheHits = data.reduce((sum, item) => sum + (item.cache_hit_count || 0), 0);
  const cacheHitPercentage = totalQueries > 0 ? (cacheHits / totalQueries) * 100 : 0;
  const nonCacheHitPercentage = 100 - cacheHitPercentage;
  
  const chartData = {
    labels: ['Cache Hit', 'Cache Miss'],
    datasets: [{
      data: [cacheHitPercentage, nonCacheHitPercentage],
      backgroundColor: [
        'rgba(25, 135, 84, 0.7)',
        'rgba(220, 53, 69, 0.7)'
      ],
      borderWidth: 1
    }]
  };
  
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'right'
      }
    }
  };
  
  // Create or update the chart
  const ctx = document.getElementById('cacheHitChart').getContext('2d');
  
  if (cacheHitChart) {
    cacheHitChart.data = chartData;
    cacheHitChart.options = chartOptions;
    cacheHitChart.update();
  } else {
    cacheHitChart = new Chart(ctx, {
      type: 'doughnut',
      data: chartData,
      options: chartOptions
    });
  }
}

/**
 * Update the query history table with the current data
 * @param {Array} data - The data to use for the table
 */
function updateTable(data) {
  // Clear the table
  queriesTableElement.innerHTML = '';
  
  // Aggregate queries by dataset and user
  const datasetUserSummary = {};
  
  // Process each item
  data.forEach(item => {
    // Get user/service account
    const user = item.service_account || item.user_email || 'Unknown';
    const userDisplay = user.split('@')[0]; // Shorter display format
    
    // Check if we have recent_queries array
    if (item.recent_queries && Array.isArray(item.recent_queries)) {
      // Process each query and aggregate by dataset and user
      item.recent_queries.forEach(query => {
        // Extract dataset from query if available
        let dataset = 'Unknown';
        if (query.query_text) {
          // Try to extract dataset from query text using regex
          const datasetMatch = query.query_text.match(/FROM\s+`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?/i);
          if (datasetMatch && datasetMatch[1]) {
            dataset = datasetMatch[1];
          }
        }
        
        // Create a unique key for dataset+user combination
        const key = `${dataset}|${user}`;
        
        // Create dataset+user key if it doesn't exist
        if (!datasetUserSummary[key]) {
          datasetUserSummary[key] = {
            dataset: dataset,
            user: user,
            userDisplay: userDisplay,
            query_count: 0,
            total_bytes_processed: 0,
            total_cost: 0,
            cache_hit_count: 0,
            error_count: 0,
            last_queried: new Date(0)
          };
        }
        
        // Update dataset+user summary
        datasetUserSummary[key].query_count++;
        datasetUserSummary[key].total_bytes_processed += (query.total_bytes_processed || 0);
        datasetUserSummary[key].total_cost += (query.query_cost_usd || 0);
        datasetUserSummary[key].cache_hit_count += (query.cache_hit ? 1 : 0);
        datasetUserSummary[key].error_count += (query.has_error ? 1 : 0);
        
        // Update last queried timestamp
        const queryTimestamp = new Date(query.timestamp);
        if (queryTimestamp > datasetUserSummary[key].last_queried) {
          datasetUserSummary[key].last_queried = queryTimestamp;
        }
      });
    } else if (item.dataset_costs && Array.isArray(item.dataset_costs)) {
      // If we have dataset_costs, use that for aggregation
      item.dataset_costs.forEach(ds => {
        const dataset = ds.dataset || 'Unknown';
        
        // Create a unique key for dataset+user combination
        const key = `${dataset}|${user}`;
        
        // Create dataset+user key if it doesn't exist
        if (!datasetUserSummary[key]) {
          datasetUserSummary[key] = {
            dataset: dataset,
            user: user,
            userDisplay: userDisplay,
            query_count: 0,
            total_bytes_processed: 0,
            total_cost: 0,
            cache_hit_count: 0,
            error_count: 0,
            last_queried: new Date(item.date)
          };
        }
        
        // Update dataset+user summary
        datasetUserSummary[key].total_bytes_processed += (ds.bytes_processed || 0);
        datasetUserSummary[key].total_cost += (ds.dataset_cost_usd || 0);
      });
    } else {
      // Legacy data format - create a summary record for "Unknown" dataset
      const dataset = 'Unknown';
      
      // Create a unique key for dataset+user combination
      const key = `${dataset}|${user}`;
      
      // Create dataset+user key if it doesn't exist
      if (!datasetUserSummary[key]) {
        datasetUserSummary[key] = {
          dataset: dataset,
          user: user,
          userDisplay: userDisplay,
          query_count: 0,
          total_bytes_processed: 0,
          total_cost: 0,
          cache_hit_count: 0,
          error_count: 0,
          last_queried: new Date(item.date)
        };
      }
      
      // Update dataset+user summary
      datasetUserSummary[key].query_count += (item.query_count || 0);
      datasetUserSummary[key].total_bytes_processed += (item.total_bytes_processed || 0);
      datasetUserSummary[key].total_cost += (item.estimated_cost_usd || 0);
      datasetUserSummary[key].cache_hit_count += (item.cache_hit_count || 0);
      datasetUserSummary[key].error_count += (item.error_count || 0);
    }
  });
  
  // Convert to array and sort by date (most recent first) and then by data processed (highest first)
  const sortedEntries = Object.values(datasetUserSummary)
    .sort((a, b) => {
      // First compare by date (most recent first)
      const dateComparison = b.last_queried - a.last_queried;
      if (dateComparison !== 0) return dateComparison;
      
      // If dates are the same, compare by data processed (highest first)
      return b.total_bytes_processed - a.total_bytes_processed;
    });
  
  // Add rows to the table
  sortedEntries.forEach(entry => {
    const row = document.createElement('tr');
    
    const bytesGB = (entry.total_bytes_processed || 0) / Math.pow(1024, 3);
    const cacheHitPercentage = entry.query_count > 0 
      ? (entry.cache_hit_count / entry.query_count * 100) 
      : 0;
    
    row.innerHTML = `
      <td style="word-break: break-word; max-width: 200px;"><code>${entry.dataset}</code></td>
      <td><code>${entry.userDisplay}</code></td>
      <td>${entry.query_count || 0} queries</td>
      <td>${bytesGB.toFixed(2)} GB</td>
      <td>${formatCurrency(entry.total_cost)}</td>
      <td>${cacheHitPercentage.toFixed(1)}%</td>
      <td>${entry.last_queried.toLocaleDateString()}</td>
    `;
    
    queriesTableElement.appendChild(row);
  });
  
  // If no entries found, show message
  if (sortedEntries.length === 0) {
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="7" class="text-center">No query data available</td>`;
    queriesTableElement.appendChild(row);
  }
}

/**
 * Show loading indicators
 */
function showLoading() {
  document.querySelectorAll('.card-body').forEach(element => {
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
 * @param {string} [message] - Optional custom message to display
 */
function showEmptyState(message) {
  hideLoading();
  
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
      sampleButton.addEventListener('click', () => {
        showSampleData();
      });
    }
  }, 0);
}

/**
 * Reset all charts to empty state
 */
function resetCharts() {
  if (costTrendChart) {
    costTrendChart.destroy();
    costTrendChart = null;
  }
  
  if (userCostChart) {
    userCostChart.destroy();
    userCostChart = null;
  }
  
  if (cacheHitChart) {
    cacheHitChart.destroy();
    cacheHitChart = null;
  }
}

/**
 * Update the dataset cost table
 * @param {Array} data - The data to use for the table
 */
function updateDatasetTable(data) {
  // Extract and aggregate dataset costs from all users
  const datasetCosts = {};
  let totalCost = 0;
  
  // Process each item
  data.forEach(item => {
    // Add to total cost for percentage calculation
    totalCost += (item.estimated_cost_usd || 0);
    
    // Process dataset_costs array if it exists
    if (item.dataset_costs && Array.isArray(item.dataset_costs)) {
      item.dataset_costs.forEach(ds => {
        const datasetName = ds.dataset;
        
        if (!datasetCosts[datasetName]) {
          datasetCosts[datasetName] = {
            bytes: 0,
            cost: 0
          };
        }
        
        datasetCosts[datasetName].bytes += (ds.bytes_processed || 0);
        datasetCosts[datasetName].cost += (ds.dataset_cost_usd || 0);
      });
    }
  });
  
  // Convert to array and sort by cost
  const sortedDatasets = Object.entries(datasetCosts)
    .sort((a, b) => b[1].cost - a[1].cost)
    .slice(0, 10); // Show top 10 datasets
  
  // Clear the table
  datasetTableElement.innerHTML = '';
  
  // Use the global formatting functions
  
  // Add rows to the table
  sortedDatasets.forEach(([dataset, data]) => {
    const row = document.createElement('tr');
    
    row.innerHTML = `
      <td><code>${dataset}</code></td>
      <td>${formatBytes(data.bytes)}</td>
      <td>${formatCurrency(data.cost)}</td>
      <td>${formatPercentage(data.cost, totalCost)}</td>
    `;
    
    datasetTableElement.appendChild(row);
  });
  
  // If no datasets found, show message
  if (sortedDatasets.length === 0) {
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="4" class="text-center">No dataset cost information available</td>`;
    datasetTableElement.appendChild(row);
  }
}

/**
 * Show sample data for demonstration purposes
 */
function showSampleData() {
  // Generate sample data
  const sampleData = generateSampleData();
  costData = sampleData;
  
  // Update the dashboard with sample data
  updateDashboard();
}

/**
 * Generate sample data for demonstration
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
 * Show time pattern analysis modal
 */
function showTimePatternModal() {
  // Check if we have hourly/daily breakdown data
  let hasTimeData = false;
  let hourlyData = [];
  let dailyData = [];
  
  // Collect time pattern data from all records
  if (costData && costData.length > 0) {
    costData.forEach(item => {
      if (item.hourly_breakdown && Array.isArray(item.hourly_breakdown)) {
        hasTimeData = true;
        
        // Aggregate hourly data across all days/users
        item.hourly_breakdown.forEach(hourData => {
          const hour = hourData.hour_of_day;
          const existingHour = hourlyData.find(h => h.hour === hour);
          
          if (existingHour) {
            existingHour.queries += hourData.hourly_queries || 0;
            existingHour.cost += hourData.hourly_cost || 0;
          } else {
            hourlyData.push({
              hour: hour,
              queries: hourData.hourly_queries || 0,
              cost: hourData.hourly_cost || 0
            });
          }
        });
      }
      
      if (item.daily_breakdown && Array.isArray(item.daily_breakdown)) {
        hasTimeData = true;
        
        // Aggregate daily data across all users
        item.daily_breakdown.forEach(dayData => {
          const day = dayData.day_of_week;
          const existingDay = dailyData.find(d => d.day === day);
          
          if (existingDay) {
            existingDay.queries += dayData.daily_queries || 0;
            existingDay.cost += dayData.daily_cost || 0;
          } else {
            dailyData.push({
              day: day,
              queries: dayData.daily_queries || 0,
              cost: dayData.daily_cost || 0
            });
          }
        });
      }
    });
  }
  
  // Sort data
  hourlyData.sort((a, b) => a.hour - b.hour);
  dailyData.sort((a, b) => a.day - b.day);
  
  // Create modal content
  let modalContent = '';
  
  if (hasTimeData) {
    // Create day names map (day_of_week is 1-7, 1 is Sunday)
    const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    
    modalContent = `
      <div class="modal fade" id="timePatternModal" tabindex="-1" aria-labelledby="timePatternModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-lg">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title" id="timePatternModalLabel">Query Time Patterns</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
              <div class="row mb-4">
                <div class="col-12">
                  <h6>Hourly Distribution</h6>
                  <div style="height: 300px;">
                    <canvas id="hourlyPatternChart"></canvas>
                  </div>
                </div>
              </div>
              <div class="row">
                <div class="col-12">
                  <h6>Day of Week Distribution</h6>
                  <div style="height: 300px;">
                    <canvas id="dailyPatternChart"></canvas>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
    
    // Add modal to the DOM
    const modalElement = document.createElement('div');
    modalElement.innerHTML = modalContent;
    document.body.appendChild(modalElement);
    
    // Initialize the modal
    const modal = new bootstrap.Modal(document.getElementById('timePatternModal'));
    modal.show();
    
    // Create the hourly chart
    const hourlyCtx = document.getElementById('hourlyPatternChart').getContext('2d');
    const hourlyChart = new Chart(hourlyCtx, {
      type: 'bar',
      data: {
        labels: hourlyData.map(h => `${h.hour}:00`),
        datasets: [
          {
            label: 'Query Count',
            data: hourlyData.map(h => h.queries),
            backgroundColor: 'rgba(13, 110, 253, 0.5)',
            borderColor: 'rgba(13, 110, 253, 1)',
            borderWidth: 1,
            yAxisID: 'y'
          },
          {
            label: 'Cost (USD)',
            data: hourlyData.map(h => h.cost),
            backgroundColor: 'rgba(220, 53, 69, 0.5)',
            borderColor: 'rgba(220, 53, 69, 1)',
            borderWidth: 1,
            yAxisID: 'y1'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            position: 'left',
            title: {
              display: true,
              text: 'Query Count'
            }
          },
          y1: {
            beginAtZero: true,
            position: 'right',
            title: {
              display: true,
              text: 'Cost (USD)'
            },
            grid: {
              drawOnChartArea: false
            }
          }
        }
      }
    });
    
    // Create the daily chart
    const dailyCtx = document.getElementById('dailyPatternChart').getContext('2d');
    const dailyChart = new Chart(dailyCtx, {
      type: 'bar',
      data: {
        labels: dailyData.map(d => dayNames[d.day - 1]),
        datasets: [
          {
            label: 'Query Count',
            data: dailyData.map(d => d.queries),
            backgroundColor: 'rgba(13, 110, 253, 0.5)',
            borderColor: 'rgba(13, 110, 253, 1)',
            borderWidth: 1,
            yAxisID: 'y'
          },
          {
            label: 'Cost (USD)',
            data: dailyData.map(d => d.cost),
            backgroundColor: 'rgba(220, 53, 69, 0.5)',
            borderColor: 'rgba(220, 53, 69, 1)',
            borderWidth: 1,
            yAxisID: 'y1'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            position: 'left',
            title: {
              display: true,
              text: 'Query Count'
            }
          },
          y1: {
            beginAtZero: true,
            position: 'right',
            title: {
              display: true,
              text: 'Cost (USD)'
            },
            grid: {
              drawOnChartArea: false
            }
          }
        }
      }
    });
    
    // Clean up when modal is hidden
    document.getElementById('timePatternModal').addEventListener('hidden.bs.modal', function () {
      hourlyChart.destroy();
      dailyChart.destroy();
      document.body.removeChild(modalElement);
    });
  } else {
    // No time pattern data available
    alert('Time pattern data is not available. This feature requires the enhanced version of the cost monitoring query.');
  }
}

/**
 * Show table details modal with rebuild vs incremental costs
 */
function showTableDetailsModal() {
  // Check if we have table costs data
  let hasTableData = false;
  let tableData = [];
  
  // Collect all table costs
  if (costData && costData.length > 0) {
    costData.forEach(item => {
      if (item.table_costs && Array.isArray(item.table_costs)) {
        hasTableData = true;
        
        // Extract and normalize table data
        item.table_costs.forEach(table => {
          const existingTable = tableData.find(t => t.table_name === table.table_name);
          
          if (existingTable) {
            // Update existing table entry
            existingTable.total_cost += table.table_cost_usd || 0;
            existingTable.rebuild_cost += table.rebuild_cost_usd || 0;
            existingTable.incremental_cost += table.incremental_cost_usd || 0;
            existingTable.bytes_processed += table.bytes_processed || 0;
            existingTable.rebuild_count += table.rebuild_count || 0;
          } else {
            // Add new table entry
            tableData.push({
              table_name: table.table_name,
              dataset_name: table.dataset_name,
              table_id: table.table_id,
              total_cost: table.table_cost_usd || 0,
              rebuild_cost: table.rebuild_cost_usd || 0,
              incremental_cost: table.incremental_cost_usd || 0,
              bytes_processed: table.bytes_processed || 0,
              rebuild_count: table.rebuild_count || 0,
              is_rebuild_operation: table.is_rebuild_operation || false
            });
          }
        });
      }
    });
  }
  
  // Sort tables by total cost
  tableData.sort((a, b) => b.total_cost - a.total_cost);
  
  // Limit to top 100 tables
  tableData = tableData.slice(0, 100);
  
  // Create modal content
  let modalContent = '';
  
  if (hasTableData) {
    modalContent = `
      <div class="modal fade" id="tableDetailsModal" tabindex="-1" aria-labelledby="tableDetailsModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-xl">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title" id="tableDetailsModalLabel">Table Cost Details</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
              <div class="table-responsive">
                <table class="table table-sm table-striped">
                  <thead>
                    <tr>
                      <th style="max-width: 250px;">Table</th>
                      <th>Dataset</th>
                      <th>Total Cost</th>
                      <th>Rebuild Cost</th>
                      <th>Incremental Cost</th>
                      <th>% Rebuild</th>
                      <th>Rebuild Count</th>
                      <th>Data Processed</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${tableData.map(table => {
                      const rebuildPercent = table.total_cost > 0 
                        ? (table.rebuild_cost / table.total_cost * 100) 
                        : 0;
                      
                      // Format bytes
                      const bytesFormatted = formatBytes(table.bytes_processed);
                      
                      // Determine row class based on rebuild percentage
                      const rowClass = rebuildPercent > 80 ? 'table-danger' : 
                                      (rebuildPercent > 50 ? 'table-warning' : '');
                      
                      return `
                        <tr class="${rowClass}">
                          <td style="word-break: break-word; max-width: 250px;" title="${table.table_id}"><code>${table.table_id}</code></td>
                          <td>${table.dataset_name}</td>
                          <td>${formatCurrency(table.total_cost)}</td>
                          <td>${formatCurrency(table.rebuild_cost)}</td>
                          <td>${formatCurrency(table.incremental_cost)}</td>
                          <td>${rebuildPercent.toFixed(1)}%</td>
                          <td>${table.rebuild_count}</td>
                          <td>${bytesFormatted}</td>
                        </tr>
                      `;
                    }).join('')}
                  </tbody>
                </table>
              </div>
            </div>
            <div class="modal-footer">
              <div class="text-muted small">
                <ul class="mb-0">
                  <li>Rows in <span class="text-danger">red</span> have >80% of costs from rebuilds</li>
                  <li>Rows in <span class="text-warning">yellow</span> have >50% of costs from rebuilds</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
    
    // Add modal to the DOM
    const modalElement = document.createElement('div');
    modalElement.innerHTML = modalContent;
    document.body.appendChild(modalElement);
    
    // Initialize the modal
    const modal = new bootstrap.Modal(document.getElementById('tableDetailsModal'));
    modal.show();
    
    // Clean up when modal is hidden
    document.getElementById('tableDetailsModal').addEventListener('hidden.bs.modal', function () {
      document.body.removeChild(modalElement);
    });
  } else {
    // No table data available
    alert('Table details are not available. This feature requires the enhanced version of the cost monitoring query.');
  }
}

/**
 * Show user-dataset attribution modal
 */
function showUserDatasetModal() {
  // Check if we have dataset costs with user information
  let hasUserDatasetData = false;
  const userDatasetMap = new Map(); // Map of user+dataset to cost
  
  // Collect user-dataset attribution data
  if (costData && costData.length > 0) {
    costData.forEach(item => {
      // Get user/service account
      const user = item.service_account || item.user_email || 'Unknown';
      const isServiceAccount = !!item.service_account;
      
      if (item.dataset_costs && Array.isArray(item.dataset_costs)) {
        item.dataset_costs.forEach(ds => {
          hasUserDatasetData = true;
          
          // Create a unique key for this user+dataset
          const key = `${user}|${ds.dataset}`;
          
          if (userDatasetMap.has(key)) {
            // Update existing entry
            const entry = userDatasetMap.get(key);
            entry.cost += ds.dataset_cost_usd || 0;
            entry.bytes += ds.bytes_processed || 0;
          } else {
            // Add new entry
            userDatasetMap.set(key, {
              user: user,
              isServiceAccount: isServiceAccount,
              dataset: ds.dataset,
              cost: ds.dataset_cost_usd || 0,
              bytes: ds.bytes_processed || 0
            });
          }
        });
      }
    });
  }
  
  // Convert map to array and sort by cost
  let userDatasetArray = Array.from(userDatasetMap.values());
  userDatasetArray.sort((a, b) => b.cost - a.cost);
  
  // Limit to top 100 entries
  userDatasetArray = userDatasetArray.slice(0, 100);
  
  // Create modal content
  let modalContent = '';
  
  if (hasUserDatasetData) {
    modalContent = `
      <div class="modal fade" id="userDatasetModal" tabindex="-1" aria-labelledby="userDatasetModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-xl">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title" id="userDatasetModalLabel">User-Dataset Attribution</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
              <div class="table-responsive">
                <table class="table table-sm table-striped">
                  <thead>
                    <tr>
                      <th>User/Service Account</th>
                      <th>Type</th>
                      <th>Dataset</th>
                      <th>Cost (USD)</th>
                      <th>Data Processed</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${userDatasetArray.map(entry => {
                      // Format values
                      const bytesFormatted = formatBytes(entry.bytes);
                      const costFormatted = formatCurrency(entry.cost);
                      
                      // Format user display (shorter)
                      const userDisplay = entry.user.split('@')[0];
                      const userType = entry.isServiceAccount ? 
                        '<span class="badge bg-danger">Service Account</span>' : 
                        '<span class="badge bg-primary">User</span>';
                      
                      return `
                        <tr>
                          <td><code>${userDisplay}</code></td>
                          <td>${userType}</td>
                          <td>${entry.dataset}</td>
                          <td>${costFormatted}</td>
                          <td>${bytesFormatted}</td>
                        </tr>
                      `;
                    }).join('')}
                  </tbody>
                </table>
              </div>
            </div>
            <div class="modal-footer">
              <div class="text-muted small">
                Shows which users and service accounts are querying each dataset and the associated costs.
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
    
    // Add modal to the DOM
    const modalElement = document.createElement('div');
    modalElement.innerHTML = modalContent;
    document.body.appendChild(modalElement);
    
    // Initialize the modal
    const modal = new bootstrap.Modal(document.getElementById('userDatasetModal'));
    modal.show();
    
    // Clean up when modal is hidden
    document.getElementById('userDatasetModal').addEventListener('hidden.bs.modal', function () {
      document.body.removeChild(modalElement);
    });
  } else {
    // No user-dataset data available
    alert('User-dataset attribution data is not available. This feature requires the enhanced version of the cost monitoring query.');
  }
}

/**
 * Show an error message
 * @param {string} message - The error message to display
 */
function showError(message) {
  console.error(message);
  alert(message);
}

/**
 * Set up event listeners for the dashboard
 */
function setupEventListeners() {
  // Period selection buttons
  periodButtons.forEach(button => {
    button.addEventListener('click', () => {
      // Update active state
      periodButtons.forEach(btn => btn.classList.remove('active'));
      button.classList.add('active');
      
      // Update active period
      activePeriod = parseInt(button.dataset.period, 10);
      
      // Update the period label in the cost card
      const costPeriodLabel = document.getElementById('costPeriodLabel');
      if (costPeriodLabel) {
        costPeriodLabel.textContent = activePeriod;
      }
      
      // Update dashboard with new period
      updateDashboard();
    });
  });
  
  // Time patterns button
  const timePatternBtn = document.getElementById('showTimePatternBtn');
  if (timePatternBtn) {
    timePatternBtn.addEventListener('click', () => {
      showTimePatternModal();
    });
  }
  
  // Table details button
  const tableDetailsBtn = document.getElementById('showTableDetailsBtn');
  if (tableDetailsBtn) {
    tableDetailsBtn.addEventListener('click', () => {
      showTableDetailsModal();
    });
  }
  
  // User-Dataset attribution button
  const userDatasetBtn = document.getElementById('showUserDatasetBtn');
  if (userDatasetBtn) {
    userDatasetBtn.addEventListener('click', () => {
      showUserDatasetModal();
    });
  }
}
