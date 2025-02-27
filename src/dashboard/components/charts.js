/**
 * Chart components for BigQuery Cost Monitor dashboard
 */

// Chart instances
let costTrendChart = null;
let userCostChart = null;
let cacheHitChart = null;

/**
 * Update the cost trend chart
 * @param {Array} dates - The dates to include in the chart
 * @param {Array} data - The data to use for the chart
 * @returns {Object} - The chart instance
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
  
  return costTrendChart;
}

/**
 * Update the user cost chart
 * @param {Array} data - The data to use for the chart
 * @returns {Object} - The chart instance
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
  
  return userCostChart;
}

/**
 * Update the cache hit chart
 * @param {Array} data - The data to use for the chart
 * @returns {Object} - The chart instance
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
  
  return cacheHitChart;
}

/**
 * Update all charts with the current data
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
 * Create time pattern charts in a modal
 * @param {Array} hourlyData - Hourly breakdown data
 * @param {Array} dailyData - Daily breakdown data
 * @returns {Object} - Object containing chart instances
 */
function createTimePatternCharts(hourlyData, dailyData) {
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
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
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
  
  return { hourlyChart, dailyChart };
}

// Export chart functions
export {
  updateCostTrendChart,
  updateUserCostChart,
  updateCacheHitChart,
  updateCharts,
  resetCharts,
  createTimePatternCharts
};
