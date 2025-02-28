/**
 * Common formatting utilities for BigQuery Cost Monitor
 */

console.log('formatters.js loaded');

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

/**
 * Format date to a readable string
 * @param {string|Date} date - The date to format
 * @param {string} format - The format to use (short, medium, long)
 * @returns {string} - Formatted date string
 */
function formatDate(date, format = 'short') {
  if (!date) return '';

  const dateObj = typeof date === 'string' ? new Date(date) : date;

  if (isNaN(dateObj.getTime())) return '';

  switch (format) {
    case 'short':
      return dateObj.toLocaleDateString();
    case 'medium':
      return dateObj.toLocaleDateString() + ' ' + dateObj.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    case 'long':
      return dateObj.toLocaleDateString() + ' ' + dateObj.toLocaleTimeString();
    default:
      return dateObj.toLocaleDateString();
  }
}

export {
  formatBytes,
  formatCurrency,
  formatPercentage,
  formatDate
};
