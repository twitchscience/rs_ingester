#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"
eval "$(curl 169.254.169.254/latest/user-data/)"
export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export STATSD_HOSTPORT="localhost:8125"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
aws s3 cp --region us-west-2 "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

exec ./dbstorer \
  -statsPrefix="${CLOUD_APP}-dbstorer.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}" \
  -databaseURL="${INGESTER_DB_URL}"
