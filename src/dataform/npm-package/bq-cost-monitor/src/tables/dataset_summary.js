/**
 * BigQuery Dataset Cost Summary View Definition
 * 
 * This module defines a view that provides a summary of BigQuery costs by dataset.
 */

/**
 * Create the dataset cost summary view
 * @param {Object} ctx - The dataform context object
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the view in
 * @param {string} options.name - The name of the view
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created view declaration
 */
function createDatasetSummaryView(ctx, options) {
  const {
    schema,
    name,
    sourceTable
  } = options;
  
  // Create the view
  const view = ctx.publish(schema, name)
    .type("view")
    .description("Summary of BigQuery costs by dataset")
    .bigquery({
      partitionBy: "date"
    })
    .tags(["cost", "monitoring", "datasets"]);
  
  // Add column descriptions
  view.columns({
    date: "Date of the query execution (YYYY-MM-DD)",
    project_id: "Google Cloud project ID",
    dataset_name: "Full name of the dataset (project.dataset)",
    total_queries: "Number of queries accessing this dataset",
    total_bytes_processed: "Total bytes processed accessing this dataset",
    total_bytes_billed: "Total bytes billed for access to this dataset",
    total_cost_usd: "Total cost in USD attributed to this dataset",
    top_users: "Array of users/service accounts with highest usage of this dataset"
  });
  
  // Set the query
  view.query(ctx => `
    -- Dataset cost summary
    WITH dataset_usage AS (
      SELECT
        costs.date,
        costs.project_id,
        dataset_entry.dataset AS dataset_name,
        costs.user_email,
        costs.service_account,
        dataset_entry.dataset_cost_usd,
        dataset_entry.bytes_processed,
        dataset_entry.bytes_billed
      FROM 
        \${ref(schema + "." + sourceTable)} costs,
        UNNEST(costs.dataset_costs) AS dataset_entry
    )

    SELECT
      date,
      project_id,
      dataset_name,
      COUNT(DISTINCT CONCAT(user_email, IFNULL(service_account, ''))) AS unique_users,
      SUM(bytes_processed) AS total_bytes_processed,
      SUM(bytes_billed) AS total_bytes_billed,
      SUM(dataset_cost_usd) AS total_cost_usd,
      -- Get top users for this dataset
      ARRAY(
        SELECT AS STRUCT
          COALESCE(service_account, user_email) AS user,
          SUM(dataset_cost_usd) AS user_cost_usd,
          SUM(bytes_processed) AS user_bytes_processed
        FROM dataset_usage du
        WHERE
          du.date = main.date
          AND du.project_id = main.project_id
          AND du.dataset_name = main.dataset_name
        GROUP BY
          user
        ORDER BY
          user_cost_usd DESC
        LIMIT 10
      ) AS top_users
    FROM
      dataset_usage main
    GROUP BY
      date, project_id, dataset_name
    ORDER BY
      date DESC, total_cost_usd DESC
  `);
  
  return view;
}

module.exports = createDatasetSummaryView;
