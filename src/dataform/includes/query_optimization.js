/**
 * Provides SQL snippets and best practices for optimizing BigQuery queries to reduce costs
 */

// Function to generate a SQL comment with optimization tips
function optimizationTips() {
  return `
-- BigQuery Cost Optimization Tips:
-- 1. Use LIMIT when exploring data to reduce bytes processed
-- 2. Use column selection instead of SELECT * to reduce bytes processed
-- 3. Use partitioned and clustered tables when possible
-- 4. Use cache by running identical queries within 24 hours
-- 5. Use approximate aggregation functions when exact precision isn't required
-- 6. Filter early in your query to reduce the amount of data processed
-- 7. Use table wildcards with caution as they can lead to processing more data than needed
`;
}

// Function to add a cost estimation comment to a query
function addCostEstimation(query) {
  return `
-- Cost Estimation:
-- This query will process approximately X TB of data
-- Estimated cost: $Y USD (at $5 per TB)

${query}
`;
}

// Function to add project and team tagging to a query
function addQueryTagging(query, projectId, teamName) {
  return `
-- Query for project: ${projectId}
-- Team: ${teamName}
-- Generated by cost-monitor

${query}
`;
}

// Function to generate a SQL snippet for checking bytes processed before running a query
function dryRunCheck() {
  return `
-- To check bytes processed before running:
/*
#standardSQL
SELECT
  total_bytes_processed,
  total_bytes_processed / POWER(1024, 4) AS terabytes_processed,
  (total_bytes_processed / POWER(1024, 4)) * 5 AS estimated_cost_usd
FROM (
  SELECT total_bytes_processed
  FROM \`region-us\`.__TABLES__
  WHERE table_id = 'your_table'
)
*/
`;
}

module.exports = {
  optimizationTips,
  addCostEstimation,
  addQueryTagging,
  dryRunCheck
};
