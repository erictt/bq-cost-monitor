/**
 * BigQuery Cost Monitor - Dashboard Application
 * 
 * This is the main entry point for the dashboard application.
 * It coordinates the various components and manages the overall state.
 */

console.log('app.js loaded');

// Import components
import {
  updateCharts,
  resetCharts
} from './components/charts.js';

import {
  updateQueriesTable,
  updateDatasetTable
} from './components/tables.js';

import {
  updateSummaryMetrics,
  generateSampleData,
  showLoading,
  hideLoading,
  showEmptyState,
} from './components/metrics.js';

import {
  loadProjects,
  loadCostData,
  filterDataByPeriod
} from './components/data.js';

// Import event listeners
import { setupEventListeners } from './eventListeners.js';

/**
 * Dashboard state management
 */
class DashboardState {
  constructor() {
    this.currentProject = null;
    this.costData = [];
    this.activePeriod = 14; // Default to 14 days

    // DOM element references
    this.domElements = {
      projectDropdown: document.getElementById('projectDropdown'),
      projectList: document.getElementById('projectList'),
      totalCostElement: document.getElementById('totalCost'),
      dataProcessedElement: document.getElementById('dataProcessed'),
      queryCountElement: document.getElementById('queryCount'),
      queriesTableElement: document.getElementById('queriesTable'),
      datasetTableElement: document.getElementById('datasetTable'),
      periodButtons: document.querySelectorAll('[data-period]')
    };
  }

  // Getter for filtered data based on active period
  getFilteredData() {
    return filterDataByPeriod(this.costData, this.activePeriod);
  }

  // Update the active period
  updateActivePeriod(period) {
    this.activePeriod = period;
  }
}

// Create dashboard state instance and expose it to window for event listeners
const dashboardState = new DashboardState();
window.dashboardState = dashboardState;

// Initialize the dashboard
document.addEventListener('DOMContentLoaded', initializeDashboard);

/**
 * Initialize the dashboard
 */
async function initializeDashboard() {
  // Load projects
  const projects = await loadProjects();

  // Populate the project dropdown
  populateProjectDropdown(projects);

  // Select the first project by default
  if (projects.length > 0) {
    selectProject(projects[0]);
  } else {
    showEmptyState({
      totalCostElement: dashboardState.domElements.totalCostElement,
      dataProcessedElement: dashboardState.domElements.dataProcessedElement,
      queryCountElement: dashboardState.domElements.queryCountElement,
      queriesTableElement: dashboardState.domElements.queriesTableElement,
      datasetTableElement: dashboardState.domElements.datasetTableElement
    }, null, resetCharts, showSampleData);
  }

  // Set up event listeners
  setupEventListeners({
    periodButtons: dashboardState.domElements.periodButtons,
    activePeriod: dashboardState.activePeriod,
    updateActivePeriod: (period) => dashboardState.updateActivePeriod(period),
    updateDashboard: updateDashboard,
    costData: dashboardState.costData
  });
}

/**
 * Populate the project dropdown with the list of projects
 * @param {Array} projects - The list of projects
 */
function populateProjectDropdown(projects) {
  dashboardState.domElements.projectList.innerHTML = '';

  projects.forEach(project => {
    const li = document.createElement('li');
    const a = document.createElement('a');
    a.classList.add('dropdown-item');
    a.textContent = project.name;
    a.dataset.projectId = project.id;
    a.href = '#';
    a.addEventListener('click', () => selectProject(project));
    li.appendChild(a);
    dashboardState.domElements.projectList.appendChild(li);
  });
}

/**
 * Select a project and load its data
 * @param {Object} project - The project to select
 */
function selectProject(project) {
  dashboardState.currentProject = project;
  dashboardState.domElements.projectDropdown.textContent = project.name;

  // Load the cost data for this project
  loadProjectData(project.id);
}

/**
 * Load cost data for a specific project
 * @param {string} projectId - The ID of the project
 * @param {boolean} useSampleData - Whether to use sample data if real data is unavailable 
 */
async function loadProjectData(projectId, useSampleData = false) {
  try {
    // Show loading indicators
    showLoading(document.querySelectorAll('.card-body'));

    // Load cost data from API
    const result = await loadCostData(projectId);

    // If no data is available yet
    if (!result.success) {
      if (useSampleData) {
        // Only show sample data if explicitly requested
        showSampleData();
      } else {
        // Otherwise show the empty state
        showEmptyState({
          totalCostElement: dashboardState.domElements.totalCostElement,
          dataProcessedElement: dashboardState.domElements.dataProcessedElement,
          queryCountElement: dashboardState.domElements.queryCountElement,
          queriesTableElement: dashboardState.domElements.queriesTableElement,
          datasetTableElement: dashboardState.domElements.datasetTableElement
        }, result.error, resetCharts, showSampleData);
      }
      return;
    }

    // Store the data and update the dashboard
    dashboardState.costData = result.data;
    updateDashboard();
  } catch (error) {
    console.error('Error in loadProjectData:', error);

    if (useSampleData) {
      // Only show sample data if explicitly requested
      showSampleData();
    } else {
      // Otherwise show the empty state with error
      showEmptyState({
        totalCostElement: dashboardState.domElements.totalCostElement,
        dataProcessedElement: dashboardState.domElements.dataProcessedElement,
        queryCountElement: dashboardState.domElements.queryCountElement,
        queriesTableElement: dashboardState.domElements.queriesTableElement,
        datasetTableElement: dashboardState.domElements.datasetTableElement
      }, `Error loading cost data: ${error.message}`, resetCharts, showSampleData);
    }
  }
}

/**
 * Update the dashboard with the current data
 */
function updateDashboard() {
  if (!dashboardState.costData || dashboardState.costData.length === 0) {
    showEmptyState({
      totalCostElement: dashboardState.domElements.totalCostElement,
      dataProcessedElement: dashboardState.domElements.dataProcessedElement,
      queryCountElement: dashboardState.domElements.queryCountElement,
      queriesTableElement: dashboardState.domElements.queriesTableElement,
      datasetTableElement: dashboardState.domElements.datasetTableElement
    }, null, resetCharts, showSampleData);
    return;
  }

  // Get filtered data based on the active period
  const filteredData = dashboardState.getFilteredData();

  // Update summary metrics
  updateSummaryMetrics(filteredData, {
    totalCostElement: dashboardState.domElements.totalCostElement,
    dataProcessedElement: dashboardState.domElements.dataProcessedElement,
    queryCountElement: dashboardState.domElements.queryCountElement
  });

  // Update charts
  updateCharts(filteredData);

  // Update the query table
  updateQueriesTable(filteredData, dashboardState.domElements.queriesTableElement);

  // Update dataset table
  updateDatasetTable(filteredData, dashboardState.domElements.datasetTableElement);

  // Hide loading indicators
  hideLoading();
}

/**
 * Show sample data for demonstration purposes
 */
function showSampleData() {
  // Generate sample data
  dashboardState.costData = generateSampleData();

  // Update the dashboard with sample data
  updateDashboard();
}
