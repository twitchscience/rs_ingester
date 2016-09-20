#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"
export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
SQS_POLL_WAIT="30s"  # Overidable in conf.sh
LISTENER_COUNT=5  # Overidable in conf.sh
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=$AWS_REGION # aws-cli uses AWS_DEFAULT_REGION, aws-sdk-go uses AWS_REGION
aws s3 cp "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

exec ./metadatastorer \
  -statsPrefix="${OWNER}.${CLOUD_APP}-metadatastorer.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}" \
  -databaseURL="${INGESTER_DB_URL}" \
  -sqsQueueName="${PROCESSED_FILES_SQS_QUEUE}" \
  -sqsPollWait="${SQS_POLL_WAIT}" \
  -listenerCount="${LISTENER_COUNT}" \
  -rollbarToken="${ROLLBAR_TOKEN}" \
  -rollbarEnvironment="${CLOUD_ENVIRONMENT}" \
