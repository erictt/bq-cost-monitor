/**
 * Event listeners for BigQuery Cost Monitor dashboard
 */

import { showTimePatternModal, showTableDetailsModal, showUserDatasetModal } from './modals.js';

/**
 * Set up event listeners for the dashboard
 * @param {Object} params - Parameters for setting up event listeners
 * @param {NodeList} params.periodButtons - Period selection buttons
 * @param {Function} params.updateActivePeriod - Function to update the active period
 * @param {Function} params.updateDashboard - Function to update the dashboard
 * @param {Array} params.costData - Reference to the cost data
 */
function setupEventListeners({ 
  periodButtons, 
  updateActivePeriod, 
  updateDashboard,
  costData
}) {
  // Period selection buttons
  periodButtons.forEach(button => {
    button.addEventListener('click', () => {
      // Update active state
      periodButtons.forEach(btn => btn.classList.remove('active'));
      button.classList.add('active');
      
      // Update active period
      const newPeriod = parseInt(button.dataset.period, 10);
      updateActivePeriod(newPeriod);
      
      // Update the period label in the cost card
      const costPeriodLabel = document.getElementById('costPeriodLabel');
      if (costPeriodLabel) {
        costPeriodLabel.textContent = newPeriod;
      }
      
      // Update dashboard with new period
      updateDashboard();
    });
  });
  
  // Time patterns button
  const timePatternBtn = document.getElementById('showTimePatternBtn');
  if (timePatternBtn) {
    timePatternBtn.addEventListener('click', () => {
      // Get the current state of costData from the dashboard state
      const dashboardState = window.dashboardState || { costData: [] };
      showTimePatternModal(dashboardState.costData);
    });
  }
  
  // Table details button
  const tableDetailsBtn = document.getElementById('showTableDetailsBtn');
  if (tableDetailsBtn) {
    tableDetailsBtn.addEventListener('click', () => {
      // Get the current state of costData from the dashboard state
      const dashboardState = window.dashboardState || { costData: [] };
      showTableDetailsModal(dashboardState.costData);
    });
  }
  
  // User-Dataset attribution button
  const userDatasetBtn = document.getElementById('showUserDatasetBtn');
  if (userDatasetBtn) {
    userDatasetBtn.addEventListener('click', () => {
      // Get the current state of costData from the dashboard state
      const dashboardState = window.dashboardState || { costData: [] };
      showUserDatasetModal(dashboardState.costData);
    });
  }
}

export { setupEventListeners };
