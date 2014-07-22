#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export STATSD_HOSTPORT="localhost:8125"

LOGDIR="/opt/science/redshiftIngester/data"
mkdir -p "${LOGDIR}"

exec ../rs_ingester -n_workers 5 -logging "${LOGDIR}" \
  -statsPrefix "${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}" \
  -scoopScheme=${SCOOP_PROTO} -scoopHost=${SCOOP_HOSTNAME}
