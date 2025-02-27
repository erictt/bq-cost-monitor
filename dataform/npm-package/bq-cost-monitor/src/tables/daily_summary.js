/**
 * BigQuery Daily Cost Summary View Definition
 * 
 * This module defines a view that provides a daily summary of BigQuery costs
 * across all projects.
 */

/**
 * Create the daily cost summary view
 * @param {Object} ctx - The dataform context object
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the view in
 * @param {string} options.name - The name of the view
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created view declaration
 */
function createDailySummaryView(ctx, options) {
  const {
    schema,
    name,
    sourceTable
  } = options;
  
  // Create the view
  const view = ctx.publish(schema, name)
    .type("view")
    .description("Daily summary of BigQuery costs across all projects")
    .tags(["cost", "monitoring", "summary"]);
  
  // Add column descriptions
  view.columns({
    date: "Date of the query execution (YYYY-MM-DD)",
    project_id: "Google Cloud project ID",
    total_queries: "Total number of queries executed",
    total_cache_hits: "Total number of queries that used cached results",
    total_errors: "Total number of queries that resulted in errors",
    total_bytes_processed: "Total bytes processed by all queries",
    total_bytes_billed: "Total bytes billed for all queries",
    total_estimated_cost_usd: "Total estimated cost in USD based on bytes billed",
    total_slot_hours: "Total slot hours consumed",
    avg_cache_hit_percentage: "Average percentage of queries that used cached results"
  });
  
  // Set the query - Uses explicit table references without ref() function
  const fullTableName = `\`${schema}.${sourceTable}\``;
  view.query(ctx => `
    SELECT
      date,
      project_id,
      SUM(query_count) AS total_queries,
      SUM(cache_hit_count) AS total_cache_hits,
      SUM(error_count) AS total_errors,
      SUM(total_bytes_processed) AS total_bytes_processed,
      SUM(total_bytes_billed) AS total_bytes_billed,
      SUM(estimated_cost_usd) AS total_estimated_cost_usd,
      SUM(slot_hours) AS total_slot_hours,
      ROUND(AVG(cache_hit_percentage), 2) AS avg_cache_hit_percentage
    FROM
      ${fullTableName}
    GROUP BY
      date, project_id
    ORDER BY
      date DESC, total_estimated_cost_usd DESC
  `);
  
  return view;
}

module.exports = createDailySummaryView;
