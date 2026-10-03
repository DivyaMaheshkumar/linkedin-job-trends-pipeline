import json
import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the Glue client outside the handler for connection reuse
glue_client = boto3.client('glue')


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    try:
        # 1. Parse the S3 event data
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        file_size = event["Records"][0]["s3"]["object"]["size"]
        file_size_mb = int(file_size) / (1024 * 1024)

        logger.info(
            f"New file detected: {s3_file_key} in bucket {bucket_name} ({file_size_mb:.2f} MB)")

        # Check if file size exceeds a limit
        if file_size_mb > 500:  # Adjust threshold as needed
            logger.warning(
                f"File {s3_file_key} is quite large: {file_size_mb:.2f} MB")

        # 2. Trigger the AWS Glue Job
        # Note: Use underscores instead of spaces to match standard AWS naming
        glue_job_name = 'job_postings_fact_job'

        # pass the S3 file path to Glue as a parameter
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--S3_BUCKET': bucket_name,
                '--S3_KEY': s3_file_key
            }
        )

        job_run_id = response['JobRunId']
        logger.info(
            f"Successfully started Glue job '{glue_job_name}' with JobRunId: {job_run_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Glue job {glue_job_name} started successfully',
                'JobRunId': job_run_id
            })
        }

    except Exception as e:
        logger.error(f"Error processing S3 event or starting Glue job: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
