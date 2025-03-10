/**
 * BigQuery Service Account Cost Summary View Definition
 * 
 * This module defines a view that provides a summary of BigQuery costs by service account.
 */

/**
 * Create the service account cost summary view
 * @param {Object} ctx - The dataform context object
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the view in
 * @param {string} options.name - The name of the view
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created view declaration
 */
function createServiceAccountSummaryView(ctx, options) {
  const {
    schema,
    name,
    sourceTable
  } = options;
  
  // Create the view
  const view = ctx.publish(schema, name)
    .type("view")
    .description("Summary of BigQuery costs by service account")
    .bigquery({
      partitionBy: "date"
    })
    .tags(["cost", "monitoring", "service-accounts"]);
  
  // Add column descriptions
  view.columns({
    date: "Date of the query execution (YYYY-MM-DD)",
    project_id: "Google Cloud project ID",
    service_account: "Service account that executed the queries",
    total_queries: "Number of queries executed by the service account",
    total_bytes_processed: "Total bytes processed by the service account",
    total_bytes_billed: "Total bytes billed for the service account",
    total_cost_usd: "Total cost in USD attributed to the service account",
    top_datasets: "Array of most expensive datasets used by this service account"
  });
  
  // Set the query - Uses explicit table references without ref() function
  const fullTableName = `\`${schema}.${sourceTable}\``;
  view.query(ctx => `
    -- Service account cost summary
    SELECT
      date,
      project_id,
      service_account,
      SUM(query_count) AS total_queries,
      SUM(total_bytes_processed) AS total_bytes_processed,
      SUM(total_bytes_billed) AS total_bytes_billed,
      SUM(estimated_cost_usd) AS total_cost_usd,
      -- Get aggregated dataset costs for this service account
      ARRAY(
        SELECT AS STRUCT
          dataset_entry.dataset AS dataset_name,
          SUM(dataset_entry.dataset_cost_usd) AS dataset_cost_usd,
          SUM(dataset_entry.bytes_processed) AS dataset_bytes_processed
        FROM 
          ${fullTableName} costs,
          UNNEST(costs.dataset_costs) AS dataset_entry
        WHERE
          costs.date = main.date
          AND costs.project_id = main.project_id
          AND costs.service_account = main.service_account
        GROUP BY
          dataset_name
        ORDER BY
          dataset_cost_usd DESC
        LIMIT 10
      ) AS top_datasets
    FROM
      ${fullTableName} main
    WHERE
      service_account IS NOT NULL
    GROUP BY
      date, project_id, service_account
    ORDER BY
      date DESC, total_cost_usd DESC
  `);
  
  return view;
}

module.exports = createServiceAccountSummaryView;
