/**
 * BigQuery Cost Monitor - Deployment Script
 * 
 * This script deploys the cost monitor as a Cloud Function and sets up a Cloud Scheduler job
 * to trigger it regularly. It also creates a Cloud Storage bucket for storing results.
 */

require('dotenv').config();
const { execSync } = require('child_process');
const { Scheduler } = require('@google-cloud/scheduler');
const fs = require('fs-extra');
const path = require('path');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// Parse command line arguments
const argv = yargs(hideBin(process.argv))
  .option('project', {
    alias: 'p',
    description: 'Google Cloud project ID to deploy to',
    type: 'string',
    demandOption: true
  })
  .option('region', {
    alias: 'r',
    description: 'Google Cloud region to deploy to',
    type: 'string',
    default: 'us-central1'
  })
  .option('bucket', {
    alias: 'b',
    description: 'Name of the Cloud Storage bucket to create for results',
    type: 'string'
  })
  .option('schedule', {
    alias: 's',
    description: 'Cron schedule for the Cloud Scheduler job',
    type: 'string',
    default: '0 5 * * *' // 5 AM every day
  })
  .option('memory', {
    alias: 'm',
    description: 'Memory allocation for the Cloud Function (in MB)',
    type: 'number',
    default: 256
  })
  .option('timeout', {
    alias: 't',
    description: 'Timeout for the Cloud Function (in seconds)',
    type: 'number',
    default: 540 // 9 minutes
  })
  .option('dry-run', {
    alias: 'd',
    description: 'Show commands that would be executed without actually executing them',
    type: 'boolean',
    default: false
  })
  .help()
  .alias('help', 'h')
  .argv;

// Validate project ID
if (!/^[a-z0-9-]+$/.test(argv.project)) {
  console.error('Error: Project ID must contain only lowercase letters, numbers, and hyphens');
  process.exit(1);
}

// Set default bucket name if not provided
const bucketName = argv.bucket || `${argv.project}-bq-cost-monitor`;

// Cloud Function name
const functionName = 'bq-cost-monitor';

// Source directory
const sourceDir = path.resolve(__dirname, '../..');

/**
 * Execute a shell command
 * @param {string} command - The command to execute
 * @param {boolean} dryRun - Whether to actually execute the command
 * @returns {string} - The command output
 */
function executeCommand(command, dryRun = false) {
  console.log(`\nExecuting: ${command}`);
  
  if (dryRun) {
    console.log('[DRY RUN] Command would be executed');
    return '';
  }
  
  try {
    const output = execSync(command, { 
      stdio: ['inherit', 'pipe', 'inherit'],
      encoding: 'utf-8'
    });
    return output.trim();
  } catch (error) {
    console.error(`Command failed: ${error.message}`);
    process.exit(1);
  }
}

/**
 * Create a Cloud Storage bucket if it doesn't exist
 */
async function createBucket() {
  console.log(`\nCreating Cloud Storage bucket: ${bucketName}`);
  
  if (argv.dryRun) {
    console.log('[DRY RUN] Bucket would be created');
    return;
  }
  
  try {
    const checkBucketCmd = `gsutil ls -b gs://${bucketName} 2>/dev/null || echo "Bucket not found"`;
    const checkResult = execSync(checkBucketCmd, { encoding: 'utf-8' }).trim();
    
    if (checkResult.includes('Bucket not found')) {
      console.log(`Bucket ${bucketName} does not exist, creating it...`);
      executeCommand(`gsutil mb -p ${argv.project} -l ${argv.region} gs://${bucketName}`);
      // Set lifecycle policy to delete files older than 90 days
      const lifecycleConfig = {
        lifecycle: {
          rule: [
            {
              action: { type: 'Delete' },
              condition: { age: 90 }
            }
          ]
        }
      };
      
      const tempFile = path.join(os.tmpdir(), 'lifecycle.json');
      fs.writeFileSync(tempFile, JSON.stringify(lifecycleConfig));
      
      executeCommand(`gsutil lifecycle set ${tempFile} gs://${bucketName}`);
      fs.unlinkSync(tempFile);
    } else {
      console.log(`Bucket ${bucketName} already exists`);
    }
  } catch (error) {
    console.error(`Error creating bucket: ${error.message}`);
    process.exit(1);
  }
}

/**
 * Deploy the Cloud Function
 */
async function deployFunction() {
  console.log('\nDeploying Cloud Function...');
  
  // Create a temporary package.json and config file for deployment
  const tempDir = path.join(os.tmpdir(), 'bq-cost-monitor-deploy');
  fs.ensureDirSync(tempDir);
  
  try {
    // Copy the source files to the temp directory
    fs.copySync(sourceDir, tempDir, {
      filter: (src) => {
        // Skip node_modules, tests, and other non-essential files
        const relativePath = path.relative(sourceDir, src);
        return !relativePath.startsWith('node_modules') && 
               !relativePath.startsWith('tests') && 
               !relativePath.startsWith('.git') &&
               !relativePath.startsWith('output') &&
               !relativePath.startsWith('logs');
      }
    });
    
    // Set environment variables
    const envVars = [
      `STORAGE_BUCKET=${bucketName}`,
      `LOG_LEVEL=info`
    ];
    
    const envFlags = envVars.map(v => `--set-env-vars ${v}`).join(' ');
    
    // Deploy the function
    const deployCmd = `gcloud functions deploy ${functionName} \\
      --project=${argv.project} \\
      --region=${argv.region} \\
      --runtime=nodejs16 \\
      --source=${tempDir} \\
      --entry-point=monitorCosts \\
      --trigger-http \\
      --allow-unauthenticated=false \\
      --service-account=${functionName}@${argv.project}.iam.gserviceaccount.com \\
      --memory=${argv.memory}MB \\
      --timeout=${argv.timeout}s \\
      ${envFlags}`;
    
    executeCommand(deployCmd, argv.dryRun);
    
    // Clean up
    fs.removeSync(tempDir);
    
  } catch (error) {
    console.error(`Error deploying function: ${error.message}`);
    if (fs.existsSync(tempDir)) {
      fs.removeSync(tempDir);
    }
    process.exit(1);
  }
}

/**
 * Create a service account for the Cloud Function
 */
async function createServiceAccount() {
  console.log('\nCreating service account...');
  
  const serviceAccount = `${functionName}@${argv.project}.iam.gserviceaccount.com`;
  
  try {
    // Check if the service account already exists
    const checkCmd = `gcloud iam service-accounts describe ${serviceAccount} --project=${argv.project} 2>/dev/null || echo "Not found"`;
    const checkResult = execSync(checkCmd, { encoding: 'utf-8' }).trim();
    
    if (checkResult.includes('Not found')) {
      console.log(`Service account ${serviceAccount} does not exist, creating it...`);
      
      executeCommand(`gcloud iam service-accounts create ${functionName} \\
        --project=${argv.project} \\
        --display-name="BigQuery Cost Monitor Service Account"`, argv.dryRun);
      
      // Grant the necessary permissions
      const roles = [
        'roles/bigquery.user',
        'roles/bigquery.jobUser',
        'roles/storage.objectAdmin',
        'roles/logging.logWriter'
      ];
      
      for (const role of roles) {
        executeCommand(`gcloud projects add-iam-policy-binding ${argv.project} \\
          --member=serviceAccount:${serviceAccount} \\
          --role=${role}`, argv.dryRun);
      }
    } else {
      console.log(`Service account ${serviceAccount} already exists`);
    }
  } catch (error) {
    console.error(`Error creating service account: ${error.message}`);
    process.exit(1);
  }
}

/**
 * Create a Cloud Scheduler job to trigger the Cloud Function
 */
async function createSchedulerJob() {
  console.log('\nCreating Cloud Scheduler job...');
  
  const jobName = `${functionName}-trigger`;
  
  if (argv.dryRun) {
    console.log(`[DRY RUN] Would create Cloud Scheduler job ${jobName} with schedule "${argv.schedule}"`);
    return;
  }
  
  try {
    // Get the function URL
    const getFunctionUrlCmd = `gcloud functions describe ${functionName} \\
      --project=${argv.project} \\
      --region=${argv.region} \\
      --format="value(httpsTrigger.url)"`;
    
    const functionUrl = execSync(getFunctionUrlCmd, { encoding: 'utf-8' }).trim();
    
    // Create a service account for the scheduler if it doesn't exist
    const schedulerServiceAccount = `${functionName}-scheduler@${argv.project}.iam.gserviceaccount.com`;
    
    const checkSchedulerSACmd = `gcloud iam service-accounts describe ${schedulerServiceAccount} --project=${argv.project} 2>/dev/null || echo "Not found"`;
    const checkResult = execSync(checkSchedulerSACmd, { encoding: 'utf-8' }).trim();
    
    if (checkResult.includes('Not found')) {
      console.log(`Creating service account for scheduler: ${schedulerServiceAccount}`);
      
      executeCommand(`gcloud iam service-accounts create ${functionName}-scheduler \\
        --project=${argv.project} \\
        --display-name="BigQuery Cost Monitor Scheduler Service Account"`);
      
      // Grant the necessary permissions
      executeCommand(`gcloud projects add-iam-policy-binding ${argv.project} \\
        --member=serviceAccount:${schedulerServiceAccount} \\
        --role=roles/cloudfunctions.invoker`);
    }
    
    // Check if the job already exists
    const listJobsCmd = `gcloud scheduler jobs list \\
      --project=${argv.project} \\
      --location=${argv.region} \\
      --filter="name:jobs/${jobName}" \\
      --format="value(name)" 2>/dev/null || echo ""`;
    
    const existingJob = execSync(listJobsCmd, { encoding: 'utf-8' }).trim();
    
    if (existingJob) {
      console.log(`Updating existing scheduler job: ${jobName}`);
      
      executeCommand(`gcloud scheduler jobs update http ${jobName} \\
        --project=${argv.project} \\
        --location=${argv.region} \\
        --schedule="${argv.schedule}" \\
        --uri="${functionUrl}" \\
        --oidc-service-account-email=${schedulerServiceAccount} \\
        --oidc-token-audience="${functionUrl}" \\
        --http-method=POST \\
        --message-body='{"force": true}'`);
    } else {
      console.log(`Creating new scheduler job: ${jobName}`);
      
      executeCommand(`gcloud scheduler jobs create http ${jobName} \\
        --project=${argv.project} \\
        --location=${argv.region} \\
        --schedule="${argv.schedule}" \\
        --uri="${functionUrl}" \\
        --oidc-service-account-email=${schedulerServiceAccount} \\
        --oidc-token-audience="${functionUrl}" \\
        --http-method=POST \\
        --message-body='{"force": true}'`);
    }
    
    console.log(`Cloud Scheduler job ${jobName} created/updated with schedule "${argv.schedule}"`);
    
  } catch (error) {
    console.error(`Error creating scheduler job: ${error.message}`);
    process.exit(1);
  }
}

/**
 * Main function to run the deployment
 */
async function deploy() {
  console.log('BigQuery Cost Monitor - Deployment');
  console.log('=================================');
  console.log(`Project: ${argv.project}`);
  console.log(`Region: ${argv.region}`);
  console.log(`Bucket: ${bucketName}`);
  console.log(`Schedule: ${argv.schedule}`);
  console.log(`Memory: ${argv.memory} MB`);
  console.log(`Timeout: ${argv.timeout} seconds`);
  console.log(`Dry run: ${argv.dryRun ? 'Yes' : 'No'}`);
  
  try {
    // Create the service account
    await createServiceAccount();
    
    // Create the bucket
    await createBucket();
    
    // Deploy the function
    await deployFunction();
    
    // Create the scheduler job
    await createSchedulerJob();
    
    console.log('\nDeployment completed successfully!');
    console.log(`\nFunction: ${functionName}`);
    console.log(`Bucket: gs://${bucketName}`);
    console.log(`Schedule: ${argv.schedule}`);
    
    if (!argv.dryRun) {
      console.log('\nYou can manually trigger the function with:');
      console.log(`gcloud functions call ${functionName} --project=${argv.project} --region=${argv.region} --data='{"force": true}'`);
    }
    
  } catch (error) {
    console.error(`Deployment failed: ${error.message}`);
    process.exit(1);
  }
}

// Run the deployment
deploy();