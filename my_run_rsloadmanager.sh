#!/bin/bash --
set -e -u -o pipefail

NO_WORK_DELAY="1m"  # Overidable in conf.sh
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=$AWS_REGION # aws-cli uses AWS_DEFAULT_REGION, aws-sdk-go uses AWS_REGION
source conf.sh

./rs_ingester \
  -n_workers 5 \
  -statsPrefix="local" \
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
  -rollbarEnvironment="local" \
  -offpeakStartHour="${OFFPEAK_START_HOUR}" \
  -offpeakDurationHours="${OFFPEAK_DURATION_HOURS}" \
  -onpeakMigrationTimeoutMs="${ONPEAK_MIGRATION_TIMEOUT_MILLISECONDS}" \
  -offpeakMigrationTimeoutMs="${OFFPEAK_MIGRATION_TIMEOUT_MILLISECONDS}" \
  -reporterPollPeriod="${REPORTER_POLL_PERIOD}" \
