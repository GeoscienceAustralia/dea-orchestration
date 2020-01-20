
# NCI Monitoring

## Function 1 - PBS Email Monitoring

Uses AWS Simple Email Service (SES) to receive emails from the NCI on job completion.

Must run in *us-west-2* as SES is not available in Australia.

## Function - GitHub Repo Stats

Records GitHub statistics into ElasticSearch every 12 hours for a list of DEA repos.

## Function - NCI Quota Monitor

Record NCI Quotas.

## Legacy NCI Job Monitoring
In dir `legacy_job_monitor/`

Originally run every 5 minutes on `raijin` to track which PBS jobs were being
run for each DEA project, and monitor their resource usage.

With **gadi** this is no longer possible, as jobs are only visible to the submitting user.

Monitor Digital Earth Australia processes running at the NCI

Sequence of steps before and after `serverless deploy`:

1) Create the SQS queue within your service (if sqs queue does not exist)
2) Add permissions on the SQS queue to allow S3 to publish notifications
    https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-lambda-function-trigger.html
    https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-basic-architecture.html
3) Enable and attach event notifications for the desired S3 Bucket to SQS queue
4) npm install
5) sls deploy --aws-profile <Existing AWS Profile Name> -vv -s prod
