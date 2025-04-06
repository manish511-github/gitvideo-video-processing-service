#!/bin/bash
set -ex  # Enable debugging and exit on error

echo "Waiting for LocalStack to be fully ready..."
until curl -s http://localhost:4566/_localstack/health | grep "\"sqs\": \"available\""; do
  sleep 2
done

echo "Creating SQS queue with explicit attributes..."
awslocal sqs create-queue \
  --queue-name processing-queue \
  --attributes '{"VisibilityTimeout":"3600","FifoQuezue":"false"}'

echo "Creating S3 buckets..."
awslocal s3 mb s3://video-raw || true
awslocal s3 mb s3://video-processed || true

echo "Configuring S3 notifications..."
awslocal s3api put-bucket-notification-configuration \
  --bucket video-raw \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:processing-queue",
      "Events": ["s3:ObjectCreated:*"]
    }]
  }'

echo "Verification:"
awslocal sqs list-queues
awslocal s3api get-bucket-notification-configuration --bucket video-raw

echo "INIT COMPLETE"