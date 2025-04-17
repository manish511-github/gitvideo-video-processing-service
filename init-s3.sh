#!/bin/bash

set -e

echo "Creating SQS queue with explicit attributes..."
awslocal sqs create-queue \
  --queue-name processing-queue \
  --attributes '{"VisibilityTimeout":"3600"}'

# Wait for SQS to be available
echo "Waiting for SQS queue to be ready..."
until awslocal sqs get-queue-url --queue-name processing-queue > /dev/null 2>&1; do
  echo "Waiting..."
  sleep 1
done

echo "Creating S3 buckets..."
awslocal s3 mb s3://video-raw || true
awslocal s3 mb s3://video-processed || true

# echo "Configuring S3 notifications..."
# awslocal s3api put-bucket-notification-configuration \
#   --bucket video-raw \
#   --notification-configuration '{
#     "QueueConfigurations": [{
#       "QueueArn": "arn:aws:sqs:us-east-1:000000000000:processing-queue",
#       "Events": ["s3:ObjectCreated:*"]
#     }]
#   }'

echo "Verification:"
awslocal sqs list-queues
awslocal s3api get-bucket-notification-configuration --bucket video-raw

echo "Setting CORS configuration for bucket: video-raw"
awslocal s3api put-bucket-cors --bucket video-raw --cors-configuration '{
  "CORSRules":[
    {
      "AllowedHeaders":["*"],
      "AllowedMethods":["GET","PUT","POST","DELETE","HEAD"],
      "AllowedOrigins":["*"],
      "ExposeHeaders":["ETag"],
      "MaxAgeSeconds":3000
    }
  ]
}'
awslocal s3api put-bucket-cors --bucket video-processed --cors-configuration '{
  "CORSRules":[
    {
      "AllowedHeaders":["*"],
      "AllowedMethods":["GET","PUT","POST","DELETE","HEAD"],
      "AllowedOrigins":["*"],
      "ExposeHeaders":["ETag"],
      "MaxAgeSeconds":3000
    }
  ]
}'

echo "INIT COMPLETE"