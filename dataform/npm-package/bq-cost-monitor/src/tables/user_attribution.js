/**
 * BigQuery User Dataset Attribution Table Definition
 * 
 * This module defines a table that provides a cross-tabulation of which users/service accounts
 * are querying which datasets and the associated costs.
 */

/**
 * Create the user dataset attribution table
 * @param {Object} ctx - The dataform context object
 * @param {Object} options - Configuration options
 * @param {string} options.schema - The schema to create the table in
 * @param {string} options.name - The name of the table
 * @param {string} options.sourceTable - The name of the source table
 * @returns {Object} - The created table declaration
 */
function createUserDatasetAttributionTable(ctx, options) {
  const {
    schema,
    name,
    sourceTable
  } = options;
  
  // Create the table
  const table = ctx.publish(schema, name)
    .type("table")
    .description("Cross-tabulation of which users/service accounts are querying which datasets and the associated costs")
    .bigquery({
      partitionBy: "date",
      clusterBy: ["user", "dataset_name"]
    })
    .tags(["cost", "monitoring", "datasets", "users", "attribution"]);
  
  // Add column descriptions
  table.columns({
    date: "Date of the query execution (YYYY-MM-DD)",
    project_id: "Google Cloud project ID",
    user: "User email or service account that executed the queries",
    is_service_account: "Whether this is a service account or a regular user",
    dataset_name: "Dataset name (project.dataset)",
    query_count: "Number of queries executed against this dataset",
    bytes_processed: "Total bytes processed",
    cost_usd: "Total cost in USD",
    pct_of_user_cost: "Percentage of user's total cost that comes from this dataset",
    pct_of_dataset_cost: "Percentage of dataset's total cost that comes from this user"
  });
  
  // Set the query - Uses explicit table references without ref() function
  const fullTableName = `\`${schema}.${sourceTable}\``;
  table.query(ctx => `
    -- Extract user-dataset relationships from the cost monitoring data
    WITH user_dataset_data AS (
      SELECT
        cm.date,
        cm.project_id,
        -- Normalize user identifier
        COALESCE(cm.service_account, cm.user_email) AS user,
        -- Identify if this is a service account
        (cm.service_account IS NOT NULL) AS is_service_account,
        -- Extract dataset information
        ds.dataset AS dataset_name,
        ds.bytes_processed,
        ds.dataset_cost_usd AS cost_usd,
        COUNT(*) AS query_count
      FROM 
        ${fullTableName} cm,
        UNNEST(cm.dataset_costs) AS ds
      GROUP BY
        cm.date, cm.project_id, user, is_service_account, dataset_name, ds.bytes_processed, ds.dataset_cost_usd
    )

    -- Main attribution table
    SELECT
      ud.date,
      ud.project_id,
      ud.user,
      ud.is_service_account,
      ud.dataset_name,
      SUM(ud.query_count) AS query_count,
      SUM(ud.bytes_processed) AS bytes_processed,
      SUM(ud.cost_usd) AS cost_usd,
      -- Calculate percentage of user's total cost that comes from this dataset
      ROUND(
        SUM(ud.cost_usd) / (
          SELECT SUM(ud2.cost_usd) 
          FROM user_dataset_data ud2 
          WHERE 
            ud2.date = ud.date 
            AND ud2.user = ud.user
        ) * 100, 
        2
      ) AS pct_of_user_cost,
      -- Calculate percentage of dataset's total cost that comes from this user
      ROUND(
        SUM(ud.cost_usd) / (
          SELECT SUM(ud3.cost_usd) 
          FROM user_dataset_data ud3 
          WHERE 
            ud3.date = ud.date 
            AND ud3.dataset_name = ud.dataset_name
        ) * 100, 
        2
      ) AS pct_of_dataset_cost
    FROM
      user_dataset_data ud
    GROUP BY
      date, project_id, user, is_service_account, dataset_name
    ORDER BY
      date DESC, cost_usd DESC
  `);
  
  return table;
}

module.exports = createUserDatasetAttributionTable;
