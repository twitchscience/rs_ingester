#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export STATSD_HOSTPORT="localhost:8125"

exec ./rs_ingester \
  -n_workers 5 \
  -statsPrefix="${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}" \
  -scoopURL="${SCOOP_URL}" \
  -databaseURL="${INGESTER_DB_PROD}" \
  -manifestBucket="rsingester-manifests"
