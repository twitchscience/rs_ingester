export LOAD_COUNT_TRIGGER=1
export LOAD_AGE_SECONDS=1
export TABLE_WHITELIST=""
export STATSD_HOSTPORT="statsd.internal.justin.tv:8125"
MIGRATOR_POLL_PERIOD="5s"
NO_WORK_DELAY="5s"
SQS_POLL_WAIT="5s"
INGESTER_DB_URL="postgres://twitcher:DCwgDSzWrdveCjfu1LvtBKrdjKdqMW2y@ingester-db.integration-jackgao.sci.justin.tv:5432/ingester"
RS_URL="postgres://integration:xzMnylqbI6uKmGMsyjaTZlzh6WLICfi6@r.integration-jackgao.sci.justin.tv:5439/product"
SCOOP_URL="https://scoop-integration-jackgao.dev.us-west2.twitch.tv"
BLUEPRINT_HOST="blueprint-integration-jackgao.dev.us-west2.twitch.tv"
RS_VERSION_TABLE_NAME="infra.event_versions"
PROCESSED_FILES_SQS_QUEUE="spade-compactor-integration-jackgao"
MANIFEST_BUCKET="rsingester-manifests-integration"
ROLLBAR_TOKEN="not15cadda857504a1d8f197bb67adcaff6"
WAIT_PROCESSOR_PERIOD="4s"
REPORTER_POLL_PERIOD="1s"
OFFPEAK_START_HOUR=3
OFFPEAK_DURATION_HOURS=24
ONPEAK_MIGRATION_TIMEOUT_MILLISECONDS=600000
OFFPEAK_MIGRATION_TIMEOUT_MILLISECONDS=10800000
BP_CONFIGS_BUCKET="science-blueprint-configs-integration"
BP_METADATA_CONFIGS_KEY="jackgao-event-metadata-configs.json.gz"
BP_METADATA_RELOAD_FREQUENCY="300s"
BP_METADATA_RETRY_DELAY="2s"