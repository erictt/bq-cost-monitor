/**
 * Table components for BigQuery Cost Monitor dashboard
 */

import { formatBytes, formatCurrency, formatPercentage, formatDate } from '../formatters.js';

/**
 * Update the query history table with the current data
 * @param {Array} data - The data to use for the table
 * @param {HTMLElement} tableElement - The table element to update
 */
function updateQueriesTable(data, tableElement) {
  // Clear the table
  tableElement.innerHTML = '';
  
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
      <td>${formatDate(entry.last_queried)}</td>
    `;
    
    tableElement.appendChild(row);
  });
  
  // If no entries found, show message
  if (sortedEntries.length === 0) {
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="7" class="text-center">No query data available</td>`;
    tableElement.appendChild(row);
  }
}

/**
 * Update the dataset cost table
 * @param {Array} data - The data to use for the table
 * @param {HTMLElement} tableElement - The table element to update
 */
function updateDatasetTable(data, tableElement) {
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
  tableElement.innerHTML = '';
  
  // Add rows to the table
  sortedDatasets.forEach(([dataset, data]) => {
    const row = document.createElement('tr');
    
    row.innerHTML = `
      <td><code>${dataset}</code></td>
      <td>${formatBytes(data.bytes)}</td>
      <td>${formatCurrency(data.cost)}</td>
      <td>${formatPercentage(data.cost, totalCost)}</td>
    `;
    
    tableElement.appendChild(row);
  });
  
  // If no datasets found, show message
  if (sortedDatasets.length === 0) {
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="4" class="text-center">No dataset cost information available</td>`;
    tableElement.appendChild(row);
  }
}

/**
 * Create and populate the table details modal
 * @param {Array} tableData - Array of table cost data
 * @returns {HTMLElement} - The modal element
 */
function createTableDetailsModal(tableData) {
  // Sort tables by total cost
  tableData.sort((a, b) => b.total_cost - a.total_cost);
  
  // Limit to top 100 tables
  tableData = tableData.slice(0, 100);
  
  // Create modal content
  const modalContent = `
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
  
  // Create modal element
  const modalElement = document.createElement('div');
  modalElement.innerHTML = modalContent;
  
  return modalElement;
}

/**
 * Create and populate the user-dataset attribution modal
 * @param {Array} data - The cost data
 * @returns {HTMLElement} - The modal element
 */
function createUserDatasetModal(data) {
  // Check if we have dataset costs with user information
  const userDatasetMap = new Map(); // Map of user+dataset to cost
  
  // Collect user-dataset attribution data
  data.forEach(item => {
    // Get user/service account
    const user = item.service_account || item.user_email || 'Unknown';
    const isServiceAccount = !!item.service_account;
    
    if (item.dataset_costs && Array.isArray(item.dataset_costs)) {
      item.dataset_costs.forEach(ds => {
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
  
  // Convert map to array and sort by cost
  let userDatasetArray = Array.from(userDatasetMap.values());
  userDatasetArray.sort((a, b) => b.cost - a.cost);
  
  // Limit to top 100 entries
  userDatasetArray = userDatasetArray.slice(0, 100);
  
  // Create modal content
  const modalContent = `
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
  
  // Create modal element
  const modalElement = document.createElement('div');
  modalElement.innerHTML = modalContent;
  
  return modalElement;
}

/**
 * Create and populate the time pattern modal
 * @param {Array} data - The cost data
 * @returns {Object} - Object containing modal element and data for charts
 */
function createTimePatternModal(data) {
  // Check if we have hourly/daily breakdown data
  let hasTimeData = false;
  let hourlyData = [];
  let dailyData = [];
  
  // Collect time pattern data from all records
  if (data && data.length > 0) {
    data.forEach(item => {
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
  const modalContent = `
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
  
  // Create modal element
  const modalElement = document.createElement('div');
  modalElement.innerHTML = modalContent;
  
  return {
    modalElement,
    hasTimeData,
    hourlyData,
    dailyData
  };
}

export {
  updateQueriesTable,
  updateDatasetTable,
  createTableDetailsModal,
  createUserDatasetModal,
  createTimePatternModal
};
