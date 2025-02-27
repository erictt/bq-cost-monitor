/**
 * Data loading and processing components for BigQuery Cost Monitor dashboard
 */

import { showError } from './metrics.js';

/**
 * Load the list of projects from the API
 * @returns {Promise<Array>} - Array of project objects
 */
async function loadProjects() {
  try {
    const response = await fetch('/api/projects');
    if (!response.ok) {
      throw new Error('Failed to load projects');
    }

    return await response.json();
  } catch (error) {
    console.error('Error loading projects:', error);
    showError('Failed to load projects. Please try again later.');
    return [];
  }
}

/**
 * Load cost data for a specific project
 * @param {string} projectId - The ID of the project
 * @returns {Promise<Object>} - Object containing the loaded data and any error
 */
async function loadCostData(projectId) {
  try {
    // Try to load from the API
    const response = await fetch(`/api/costs/${projectId}`);

    // If no data is available yet
    if (!response.ok) {
      console.warn(`No cost data available for project ${projectId}`);
      return {
        success: false,
        error: `No cost data available for project ${projectId}`,
        data: []
      };
    }

    // Parse the real data
    const data = await response.json();
    return {
      success: true,
      data
    };
  } catch (error) {
    console.error('Error loading cost data:', error);
    return {
      success: false,
      error: `Error loading cost data: ${error.message}`,
      data: []
    };
  }
}

/**
 * Load summary data from the API
 * @returns {Promise<Object>} - Object containing the loaded summary data and any error
 */
async function loadSummaryData() {
  try {
    const response = await fetch('/api/summary');

    if (!response.ok) {
      return {
        success: false,
        error: 'No summary data available',
        data: []
      };
    }

    const data = await response.json();
    return {
      success: true,
      data
    };
  } catch (error) {
    console.error('Error loading summary data:', error);
    return {
      success: false,
      error: `Error loading summary data: ${error.message}`,
      data: []
    };
  }
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
 * Extract table costs from the data
 * @param {Array} data - The cost data
 * @returns {Array} - Array of table cost objects
 */
function extractTableCosts(data) {
  const tableCosts = [];

  // Process each item
  data.forEach(item => {
    if (item.table_costs && Array.isArray(item.table_costs)) {
      // Extract and normalize table data
      item.table_costs.forEach(table => {
        const existingTable = tableCosts.find(t => t.table_name === table.table_name);

        if (existingTable) {
          // Update existing table entry
          existingTable.total_cost += table.table_cost_usd || 0;
          existingTable.rebuild_cost += table.rebuild_cost_usd || 0;
          existingTable.incremental_cost += table.incremental_cost_usd || 0;
          existingTable.bytes_processed += table.bytes_processed || 0;
          existingTable.rebuild_count += table.rebuild_count || 0;
        } else {
          // Add new table entry
          tableCosts.push({
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

  return tableCosts;
}

/**
 * Extract time pattern data from the cost data
 * @param {Array} data - The cost data
 * @returns {Object} - Object containing hourly and daily data
 */
function extractTimePatternData(data) {
  let hourlyData = [];
  let dailyData = [];

  // Collect time pattern data from all records
  if (data && data.length > 0) {
    data.forEach(item => {
      if (item.hourly_breakdown && Array.isArray(item.hourly_breakdown)) {
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

  return {
    hourlyData,
    dailyData,
    hasTimeData: hourlyData.length > 0 || dailyData.length > 0
  };
}

export {
  loadProjects,
  loadCostData,
  loadSummaryData,
  filterDataByPeriod,
  extractTableCosts,
  extractTimePatternData
};
