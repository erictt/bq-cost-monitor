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

// DOM elements
const projectDropdown = document.getElementById('projectDropdown');
const projectList = document.getElementById('projectList');
const totalCostElement = document.getElementById('totalCost');
const dataProcessedElement = document.getElementById('dataProcessed');
const queryCountElement = document.getElementById('queryCount');
const queriesTableElement = document.getElementById('queriesTable');
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
 */
async function loadCostData(projectId) {
  try {
    showLoading();
    
    // Try to load from the API
    const response = await fetch(`/api/costs/${projectId}`);
    
    // If no data is available yet, show sample data
    if (!response.ok) {
      showSampleData();
      return;
    }
    
    costData = await response.json();
    
    // Update the dashboard with the loaded data
    updateDashboard();
  } catch (error) {
    console.error('Error loading cost data:', error);
    showSampleData();
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
  
  // Update the table
  updateTable(filteredData);
  
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
  // Aggregate cost by user
  const costByUser = {};
  data.forEach(item => {
    const user = item.user_email || 'Unknown';
    if (!costByUser[user]) {
      costByUser[user] = 0;
    }
    costByUser[user] += (item.estimated_cost_usd || 0);
  });
  
  // Sort users by cost and take top 5
  const topUsers = Object.entries(costByUser)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  
  const chartData = {
    labels: topUsers.map(([user]) => user.split('@')[0]), // Just show username part
    datasets: [{
      label: 'Cost by User (USD)',
      data: topUsers.map(([, cost]) => cost),
      backgroundColor: [
        'rgba(13, 110, 253, 0.7)',
        'rgba(25, 135, 84, 0.7)',
        'rgba(255, 193, 7, 0.7)',
        'rgba(220, 53, 69, 0.7)',
        'rgba(108, 117, 125, 0.7)'
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
  const ctx = document.getElementById('userCostChart').getContext('2d');
  
  if (userCostChart) {
    userCostChart.data = chartData;
    userCostChart.options = chartOptions;
    userCostChart.update();
  } else {
    userCostChart = new Chart(ctx, {
      type: 'pie',
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
 * Update the table with the current data
 * @param {Array} data - The data to use for the table
 */
function updateTable(data) {
  // Sort data by date (newest first)
  const sortedData = [...data].sort((a, b) => {
    return new Date(b.date) - new Date(a.date);
  });
  
  // Limit to the most recent 10 entries
  const recentData = sortedData.slice(0, 10);
  
  // Clear the table
  queriesTableElement.innerHTML = '';
  
  // Add rows to the table
  recentData.forEach(item => {
    const row = document.createElement('tr');
    
    // Format bytes as GB
    const bytesGB = (item.total_bytes_processed || 0) / Math.pow(1024, 3);
    
    // Calculate cache hit percentage
    const cacheHitPercentage = item.query_count > 0 
      ? (item.cache_hit_count / item.query_count) * 100 
      : 0;
    
    row.innerHTML = `
      <td>${item.date}</td>
      <td>${item.user_email || 'Unknown'}</td>
      <td>${item.query_count || 0}</td>
      <td>${bytesGB.toFixed(2)} GB</td>
      <td>$${(item.estimated_cost_usd || 0).toFixed(2)}</td>
      <td>${cacheHitPercentage.toFixed(1)}%</td>
    `;
    
    queriesTableElement.appendChild(row);
  });
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
      
      // Update dashboard with new period
      updateDashboard();
    });
  });
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
 */
function showEmptyState() {
  hideLoading();
  
  // Update summary metrics with zeros
  totalCostElement.textContent = '$0.00';
  dataProcessedElement.textContent = '0 TB';
  queryCountElement.textContent = '0';
  
  // Clear the table
  queriesTableElement.innerHTML = '';
  
  // Add empty state message to the table
  const emptyRow = document.createElement('tr');
  emptyRow.innerHTML = `
    <td colspan="6">
      <div class="empty-state">
        <p>No cost data available for this project yet.</p>
        <p>Run the cost monitoring script to collect data.</p>
      </div>
    </td>
  `;
  queriesTableElement.appendChild(emptyRow);
  
  // Reset charts
  resetCharts();
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
  const users = ['user1@example.com', 'user2@example.com', 'user3@example.com'];
  
  // Generate data for the last 30 days
  for (let i = 0; i < 30; i++) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    const dateString = date.toISOString().split('T')[0];
    
    // Generate data for each user
    users.forEach(user => {
      // Random values with some variation
      const queryCount = Math.floor(Math.random() * 50) + 10;
      const cacheHitCount = Math.floor(Math.random() * queryCount);
      const bytesProcessed = Math.floor(Math.random() * 1000000000000) + 100000000000;
      const bytesBilled = bytesProcessed;
      const cost = (bytesBilled / Math.pow(1024, 4)) * 5;
      
      data.push({
        date: dateString,
        user_email: user,
        query_count: queryCount,
        cache_hit_count: cacheHitCount,
        error_count: Math.floor(Math.random() * 5),
        total_bytes_processed: bytesProcessed,
        total_bytes_billed: bytesBilled,
        estimated_cost_usd: cost,
        slot_hours: Math.random() * 10,
        cache_hit_percentage: (cacheHitCount / queryCount) * 100
      });
    });
  }
  
  return data;
}

/**
 * Show an error message
 * @param {string} message - The error message to display
 */
function showError(message) {
  console.error(message);
  alert(message);
}
