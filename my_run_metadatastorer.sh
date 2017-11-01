#!/bin/bash --
set -e -u -o pipefail

SQS_POLL_WAIT="30s"  # Overidable in conf.sh
LISTENER_COUNT=5  # Overidable in conf.sh
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=$AWS_REGION # aws-cli uses AWS_DEFAULT_REGION, aws-sdk-go uses AWS_REGION
source conf.sh

./metadatastorer/metadatastorer \
  -statsPrefix="local" \
  -databaseURL="${INGESTER_DB_URL}" \
  -sqsQueueName="${PROCESSED_FILES_SQS_QUEUE}" \
  -sqsPollWait="${SQS_POLL_WAIT}" \
  -listenerCount="${LISTENER_COUNT}" \
  -rollbarToken="${ROLLBAR_TOKEN}" \
  -rollbarEnvironment="${CLOUD_ENVIRONMENT}" \
  -bpConfigsBucket="${BP_CONFIGS_BUCKET}" \
  -bpMetadataConfigsKey="${BP_METADATA_CONFIGS_KEY}" \
  -bpMetadataReloadFrequency="${BP_METADATA_RELOAD_FREQUENCY}" \
  -bpMetadataRetryDelay="${BP_METADATA_RETRY_DELAY}" \
