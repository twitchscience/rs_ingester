#!/bin/bash --
set -e -u -o pipefail

source /etc/environment
cd -- "$(dirname -- "$0")"
export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
NO_WORK_DELAY="1m"  # Overidable in conf.sh
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=$AWS_REGION # aws-cli uses AWS_DEFAULT_REGION, aws-sdk-go uses AWS_REGION
aws s3 cp "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

exec ./rsloadmanager \
  -n_workers 5 \
  -statsPrefix="${OWNER}.${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}" \
  -databaseURL="${INGESTER_DB_URL}" \
  -manifestBucket="${MANIFEST_BUCKET}" \
  -loadCountTrigger="${LOAD_COUNT_TRIGGER}" \
  -loadAgeSeconds="${LOAD_AGE_SECONDS}" \
  -no_work_delay="${NO_WORK_DELAY}" \
  -migratorPollPeriod="${MIGRATOR_POLL_PERIOD}" \
  -waitProcessorPeriod="${WAIT_PROCESSOR_PERIOD}" \
  -blueprint_host="${BLUEPRINT_HOST}" \
  -rsURL="${RS_URL}" \
  -rollbarToken="${ROLLBAR_TOKEN}" \
  -rollbarEnvironment="${CLOUD_ENVIRONMENT}" \
  -offpeakStartHour="${OFFPEAK_START_HOUR}" \
  -offpeakDurationHours="${OFFPEAK_DURATION_HOURS}" \
  -onpeakMigrationTimeoutMs="${ONPEAK_MIGRATION_TIMEOUT_MILLISECONDS}" \
  -offpeakMigrationTimeoutMs="${OFFPEAK_MIGRATION_TIMEOUT_MILLISECONDS}" \
  -reporterPollPeriod="${REPORTER_POLL_PERIOD}" \
