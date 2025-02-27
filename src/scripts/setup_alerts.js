/**
 * BigQuery Cost Monitor - Alert Setup Script
 * 
 * This script sets up Cloud Monitoring alerts for BigQuery costs,
 * including cost thresholds, usage spikes, and failure alerts.
 */

require('dotenv').config();
const { monitoring } = require('@google-cloud/monitoring');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// Parse command line arguments
const argv = yargs(hideBin(process.argv))
  .option('project', {
    alias: 'p',
    description: 'Google Cloud project ID to set up alerts in',
    type: 'string',
    demandOption: true
  })
  .option('threshold', {
    alias: 't',
    description: 'Cost threshold in USD for alerts (per day)',
    type: 'number',
    default: 100
  })
  .option('email', {
    alias: 'e',
    description: 'Email address to send alerts to',
    type: 'string',
    demandOption: true
  })
  .option('channel-name', {
    alias: 'c',
    description: 'Name for the notification channel',
    type: 'string',
    default: 'bigquery-cost-alerts'
  })
  .help()
  .alias('help', 'h')
  .argv;

// Validate project ID
if (!/^[a-z0-9-]+$/.test(argv.project)) {
  console.error('Error: Project ID must contain only lowercase letters, numbers, and hyphens');
  process.exit(1);
}

// Validate email address
if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(argv.email)) {
  console.error('Error: Invalid email address format');
  process.exit(1);
}

// Initialize the Cloud Monitoring client
const alertClient = new monitoring.AlertPolicyServiceClient();
const notificationClient = new monitoring.NotificationChannelServiceClient();

/**
 * Create a notification channel for alerts
 * @returns {Promise<string>} - The notification channel name
 */
async function createNotificationChannel() {
  console.log(`Creating notification channel for email: ${argv.email}`);
  
  try {
    const [channels] = await notificationClient.listNotificationChannels({
      name: notificationClient.projectPath(argv.project)
    });
    
    // Check if a channel with this email already exists
    const existingChannel = channels.find(channel => 
      channel.type === 'email' && 
      channel.labels.email === argv.email
    );
    
    if (existingChannel) {
      console.log(`Using existing notification channel: ${existingChannel.displayName}`);
      return existingChannel.name;
    }
    
    // Create a new notification channel
    const [channel] = await notificationClient.createNotificationChannel({
      name: notificationClient.projectPath(argv.project),
      notificationChannel: {
        type: 'email',
        displayName: argv.channelName,
        description: 'Notification channel for BigQuery cost alerts',
        labels: {
          email: argv.email
        }
      }
    });
    
    console.log(`Created notification channel: ${channel.displayName}`);
    return channel.name;
  } catch (error) {
    console.error(`Error creating notification channel: ${error.message}`);
    process.exit(1);
  }
}

/**
 * Create an alert policy for daily BigQuery cost threshold
 * @param {string} channelName - The notification channel name
 */
async function createCostThresholdAlert(channelName) {
  console.log(`Creating cost threshold alert for $${argv.threshold} per day`);
  
  const policyName = `bigquery-daily-cost-threshold-${argv.threshold}`;
  
  try {
    const [policies] = await alertClient.listAlertPolicies({
      name: alertClient.projectPath(argv.project)
    });
    
    // Check if policy already exists
    const existingPolicy = policies.find(policy => 
      policy.displayName === policyName
    );
    
    if (existingPolicy) {
      console.log(`Updating existing alert policy: ${policyName}`);
      
      // Update the existing policy
      const [updatedPolicy] = await alertClient.updateAlertPolicy({
        alertPolicy: {
          name: existingPolicy.name,
          displayName: policyName,
          documentation: {
            content: `Alert when daily BigQuery cost exceeds $${argv.threshold}`,
            mimeType: 'text/markdown'
          },
          conditions: [
            {
              displayName: 'BigQuery Daily Cost Threshold',
              conditionThreshold: {
                filter: 'resource.type="bigquery_project" AND metric.type="bigquery.googleapis.com/query/execution_times"',
                aggregations: [
                  {
                    alignmentPeriod: { seconds: 86400 }, // 1 day
                    perSeriesAligner: 'ALIGN_SUM',
                    crossSeriesReducer: 'REDUCE_SUM'
                  }
                ],
                comparison: 'COMPARISON_GT',
                thresholdValue: argv.threshold,
                duration: { seconds: 0 },
                trigger: {
                  count: 1
                }
              }
            }
          ],
          combiner: 'OR',
          notificationChannels: [channelName],
          enabled: true
        }
      });
      
      console.log(`Updated alert policy: ${updatedPolicy.displayName}`);
    } else {
      console.log(`Creating new alert policy: ${policyName}`);
      
      // Create a new policy
      const [policy] = await alertClient.createAlertPolicy({
        name: alertClient.projectPath(argv.project),
        alertPolicy: {
          displayName: policyName,
          documentation: {
            content: `Alert when daily BigQuery cost exceeds $${argv.threshold}`,
            mimeType: 'text/markdown'
          },
          conditions: [
            {
              displayName: 'BigQuery Daily Cost Threshold',
              conditionThreshold: {
                filter: 'resource.type="bigquery_project" AND metric.type="bigquery.googleapis.com/query/execution_times"',
                aggregations: [
                  {
                    alignmentPeriod: { seconds: 86400 }, // 1 day
                    perSeriesAligner: 'ALIGN_SUM',
                    crossSeriesReducer: 'REDUCE_SUM'
                  }
                ],
                comparison: 'COMPARISON_GT',
                thresholdValue: argv.threshold,
                duration: { seconds: 0 },
                trigger: {
                  count: 1
                }
              }
            }
          ],
          combiner: 'OR',
          notificationChannels: [channelName],
          enabled: true
        }
      });
      
      console.log(`Created alert policy: ${policy.displayName}`);
    }
  } catch (error) {
    console.error(`Error creating cost threshold alert: ${error.message}`);
    console.error(error.stack);
  }
}

/**
 * Create an alert policy for cost monitor function failures
 * @param {string} channelName - The notification channel name
 */
async function createFunctionFailureAlert(channelName) {
  console.log('Creating alert for cost monitor function failures');
  
  const policyName = 'bigquery-cost-monitor-function-failure';
  
  try {
    const [policies] = await alertClient.listAlertPolicies({
      name: alertClient.projectPath(argv.project)
    });
    
    // Check if policy already exists
    const existingPolicy = policies.find(policy => 
      policy.displayName === policyName
    );
    
    const functionName = 'bq-cost-monitor';
    
    if (existingPolicy) {
      console.log(`Updating existing alert policy: ${policyName}`);
      
      // Update the existing policy
      const [updatedPolicy] = await alertClient.updateAlertPolicy({
        alertPolicy: {
          name: existingPolicy.name,
          displayName: policyName,
          documentation: {
            content: 'Alert when the BigQuery cost monitor function fails',
            mimeType: 'text/markdown'
          },
          conditions: [
            {
              displayName: 'Function Execution Failure',
              conditionThreshold: {
                filter: `resource.type="cloud_function" AND resource.labels.function_name="${functionName}" AND metric.type="cloudfunctions.googleapis.com/function/execution_count" AND metric.labels.status="error"`,
                aggregations: [
                  {
                    alignmentPeriod: { seconds: 300 }, // 5 minutes
                    perSeriesAligner: 'ALIGN_COUNT',
                    crossSeriesReducer: 'REDUCE_SUM'
                  }
                ],
                comparison: 'COMPARISON_GT',
                thresholdValue: 0,
                duration: { seconds: 0 },
                trigger: {
                  count: 1
                }
              }
            }
          ],
          combiner: 'OR',
          notificationChannels: [channelName],
          enabled: true
        }
      });
      
      console.log(`Updated alert policy: ${updatedPolicy.displayName}`);
    } else {
      console.log(`Creating new alert policy: ${policyName}`);
      
      // Create a new policy
      const [policy] = await alertClient.createAlertPolicy({
        name: alertClient.projectPath(argv.project),
        alertPolicy: {
          displayName: policyName,
          documentation: {
            content: 'Alert when the BigQuery cost monitor function fails',
            mimeType: 'text/markdown'
          },
          conditions: [
            {
              displayName: 'Function Execution Failure',
              conditionThreshold: {
                filter: `resource.type="cloud_function" AND resource.labels.function_name="${functionName}" AND metric.type="cloudfunctions.googleapis.com/function/execution_count" AND metric.labels.status="error"`,
                aggregations: [
                  {
                    alignmentPeriod: { seconds: 300 }, // 5 minutes
                    perSeriesAligner: 'ALIGN_COUNT',
                    crossSeriesReducer: 'REDUCE_SUM'
                  }
                ],
                comparison: 'COMPARISON_GT',
                thresholdValue: 0,
                duration: { seconds: 0 },
                trigger: {
                  count: 1
                }
              }
            }
          ],
          combiner: 'OR',
          notificationChannels: [channelName],
          enabled: true
        }
      });
      
      console.log(`Created alert policy: ${policy.displayName}`);
    }
  } catch (error) {
    console.error(`Error creating function failure alert: ${error.message}`);
    console.error(error.stack);
  }
}

/**
 * Main function to set up all alerts
 */
async function setupAlerts() {
  console.log('BigQuery Cost Monitor - Alert Setup');
  console.log('===================================');
  console.log(`Project: ${argv.project}`);
  console.log(`Cost threshold: $${argv.threshold} per day`);
  console.log(`Email notifications: ${argv.email}`);
  
  try {
    // Create notification channel
    const channelName = await createNotificationChannel();
    
    // Create cost threshold alert
    await createCostThresholdAlert(channelName);
    
    // Create function failure alert
    await createFunctionFailureAlert(channelName);
    
    console.log('\nAlert setup completed successfully!');
  } catch (error) {
    console.error(`Alert setup failed: ${error.message}`);
    process.exit(1);
  }
}

// Run the alert setup
setupAlerts();