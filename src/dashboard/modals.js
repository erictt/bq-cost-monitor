/**
 * Modal components for BigQuery Cost Monitor dashboard
 */

import { formatBytes, formatCurrency, formatPercentage } from './formatters.js';
import { extractTableCosts, extractTimePatternData } from './components/data.js';
import { createTimePatternCharts } from './components/charts.js';

/**
 * Show time pattern analysis modal
 * @param {Array} costData - The cost data
 */
function showTimePatternModal(costData) {
  // Extract time pattern data
  const { hourlyData, dailyData, hasTimeData } = extractTimePatternData(costData);
  
  if (!hasTimeData) {
    // No time pattern data available
    alert('Time pattern data is not available. This feature requires the enhanced version of the cost monitoring query.');
    return;
  }
  
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
  
  // Add modal to the DOM
  const modalElement = document.createElement('div');
  modalElement.innerHTML = modalContent;
  document.body.appendChild(modalElement);
  
  // Initialize the modal
  const modal = new bootstrap.Modal(document.getElementById('timePatternModal'));
  modal.show();
  
  // Create the charts
  const { hourlyChart, dailyChart } = createTimePatternCharts(hourlyData, dailyData);
  
  // Clean up when modal is hidden
  document.getElementById('timePatternModal').addEventListener('hidden.bs.modal', function () {
    hourlyChart.destroy();
    dailyChart.destroy();
    document.body.removeChild(modalElement);
  });
}

/**
 * Show table details modal with rebuild vs incremental costs
 * @param {Array} costData - The cost data
 */
function showTableDetailsModal(costData) {
  // Extract table costs
  const tableData = extractTableCosts(costData);
  
  if (tableData.length === 0) {
    // No table data available
    alert('Table details are not available. This feature requires the enhanced version of the cost monitoring query.');
    return;
  }
  
  // Sort tables by total cost
  tableData.sort((a, b) => b.total_cost - a.total_cost);
  
  // Limit to top 100 tables
  const limitedTableData = tableData.slice(0, 100);
  
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
                  ${limitedTableData.map(table => {
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
}

/**
 * Show user-dataset attribution modal
 * @param {Array} costData - The cost data
 */
function showUserDatasetModal(costData) {
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
  
  if (!hasUserDatasetData) {
    // No user-dataset data available
    alert('User-dataset attribution data is not available. This feature requires the enhanced version of the cost monitoring query.');
    return;
  }
  
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
}

export {
  showTimePatternModal,
  showTableDetailsModal,
  showUserDatasetModal
};
