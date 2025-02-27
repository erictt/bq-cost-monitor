# BigQuery Cost Monitor

A comprehensive solution for monitoring BigQuery usage costs per project with Dataform integration and Google Cloud deployment options.

## Overview

BigQuery Cost Monitor helps teams track and analyze their BigQuery usage and associated costs. It provides:

- Detailed cost tracking per project, user, and query
- Integration with Dataform for SQL workflow management
- Interactive dashboard for visualizing cost trends
- Optimization suggestions to reduce costs
- Automated deployment as Cloud Functions
- Cost alerts and monitoring

## Features

- **Cost Tracking**: Monitor BigQuery usage costs across multiple projects
- **Usage Analytics**: Track bytes processed, query count, and cache hit rates
- **User Attribution**: Identify which users or teams are generating the most costs
- **Dataform Integration**: Seamlessly integrate with Dataform projects
- **Interactive Dashboard**: Visualize cost trends and usage patterns
- **Query Optimization**: Get suggestions for optimizing queries to reduce costs
- **Cloud Deployment**: Deploy as a managed Cloud Function with scheduled execution
- **Cost Alerts**: Setup threshold-based alerts for cost monitoring
- **Logging & Monitoring**: Structured logging and error alerting

## Project Structure

```
bq-cost-monitor/
├── config/                  # Configuration files
│   └── projects.json        # Project settings
├── src/
│   ├── queries/             # SQL queries for cost monitoring
│   │   ├── usage_query.sql  # Query to extract usage data
│   │   └── cost_query.sql   # Query to calculate costs
│   ├── dataform/            # Dataform integration
│   │   ├── definitions/     # Dataform table definitions
│   │   └── includes/        # Reusable SQL snippets
│   ├── scripts/             # Utility scripts
│   │   ├── run_monitor.js   # Script to run monitoring
│   │   ├── cloud_function.js # Cloud Function entry point
│   │   ├── deploy.js        # Deployment script
│   │   ├── setup_alerts.js  # Alert configuration script
│   │   └── serve_dashboard.js # Script to serve the dashboard
│   └── dashboard/           # Web dashboard
│       ├── index.html       # Dashboard UI
│       ├── styles.css       # Dashboard styling
│       └── app.js           # Dashboard logic
├── logs/                    # Log files
└── output/                  # Output directory for monitoring results
```

## Getting Started

### Prerequisites

- Node.js (v14 or later)
- Google Cloud SDK
- BigQuery access to the projects you want to monitor

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/erictt/bq-cost-monitor.git
   cd bq-cost-monitor
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Configure your projects:
   Edit `config/projects.json` to include the Google Cloud projects you want to monitor.

### Usage

#### Running the Cost Monitor Locally

To collect cost data for your configured projects:

```
npm start
```

This will:
- Query the BigQuery INFORMATION_SCHEMA for each project
- Calculate cost estimates based on usage
- Save the results to the output directory

#### Viewing the Dashboard

To start the dashboard server:

```
npm run dashboard
```

Then open your browser to http://localhost:3000

For development with automatic reloading:

```
npm run dev
```

### Cloud Deployment

You can deploy the cost monitor as a Cloud Function that runs on a schedule:

```
npm run deploy -- --project=your-gcp-project-id --region=us-central1
```

This will:
1. Create a service account with necessary permissions
2. Create a Cloud Storage bucket for result storage
3. Deploy the cost monitor as a Cloud Function
4. Set up a Cloud Scheduler job to trigger the function daily

Additional deployment options:
```
npm run deploy -- --help
```

### Setting Up Alerts

To set up cost threshold alerts:

```
npm run alerts -- --project=your-gcp-project-id --threshold=100 --email=alerts@example.com
```

This will create:
1. A notification channel for email alerts
2. A cost threshold alert that triggers when daily costs exceed the threshold
3. A function failure alert that triggers if the monitor fails to run

### Dataform Integration

The `src/dataform` directory contains definitions and includes that can be used in your Dataform projects:

- `definitions/bigquery_cost_monitoring.sqlx`: Creates a table for storing cost data
- `definitions/daily_cost_summary.sqlx`: Creates a view for daily cost summaries
- `includes/query_optimization.js`: Provides SQL snippets for optimizing queries

To use these in your Dataform project, copy the files to your Dataform project directory.

## Cost Optimization Tips

1. **Use column selection** instead of `SELECT *` to reduce bytes processed
2. **Filter early** in your query to reduce the amount of data processed
3. **Use partitioned and clustered tables** when possible
4. **Leverage the cache** by running identical queries within 24 hours
5. **Use approximate aggregation functions** when exact precision isn't required

## Environment Variables

The application supports the following environment variables:

- `CONFIG_PATH`: Path to the configuration file (default: `config/projects.json`)
- `BQ_LOCATION`: Default BigQuery location (default: `US`)
- `HISTORY_DAYS`: Number of days of history to query (default: 30)
- `COST_PER_TB`: Cost per terabyte for calculations (default: 5.0)
- `LOG_LEVEL`: Logging level (default: `info`)
- `STORAGE_BUCKET`: GCS bucket name for Cloud Function results

## Development

### Running Tests

```
npm test
```

### Linting

```
npm run lint
```

## License

This project is licensed under the MIT License.
