***NOTE: This project is no longer being updated publicly.***

**Table of Contents**
- [Ingester](#ingester)
  - [metadatastorer](#metadatastorer)
  - [rsloadmanager](#rsloadmanager)
    - [Loaders](#loaders)
    - [Migrator](#migrator)
  - [Control](#control)
  - [License](#license)


# Ingester

The ingester manages loading processed event data into a redshift database and
migrating the schemas of the database. At the highest level, it does this by
receiving pointers to tsv files and loads them in batches, and
migrates tables if it discovers a new version.


## metadatastorer
The metadatastorer ([code](metadatastorer/main.go)) is a separate binary that has the simple task of reading messages
off an SQS queue and then writing them as rows in a postgres metadata database.
The incoming messages look like:
```json
{
    "KeyName":"spade-compacter-prod/20160729/oauth_authorize/v0/processor-data-ami-94f837f4/ip-10-192-9-216.us-west-2.compute.internal.1469832764.log.gz",
    "TableName":"oauth_authorize",
    "TableVersion":0
}
```
and get stored into the `tsv` table, whose schema is
[here](init_db/init.sql).


## rsloadmanager
The rsloadmanager ([code](main.go)) is the main binary that performs two major
functions:

1. Batch tsvs, and tell redshift to load them with manifests.
2. Migrate schemas to new versions.

### Loaders
The loaders are a pool of goroutines that manages loading the tsvs into the
redshift database.

Each goroutine does the following:
* It searches the `tsv` table for events that have `--loadAgeSeconds` old tsvs, or `--loadCountTrigger` many
rows (both configurable) and pulls the oldest to load that is the current table version.
* It then creates a row in the `manifest` table and sets the `manifest_uuid` on the rows
in `tsv` corresponding to that table-version.
* It creates a manifest in s3 of all those s3 keys (from
the `tsv` rows).
* Then it submits a `COPY` query to redshift, pointing at that manifest. If the load succeeds, the files and manifest are deleted from `tsv` and `manifest`.


### Migrator
The migrator ([code](migrator/migrator.go)) is a separate goroutine that
discovers schemas that need to be migrated, and then migrates them.

On startup,
a shared (across all the goroutines) [map](versions/versions.go) of table
name to version number is pulled from the redshift table `infra.table_version`.

The migrator does the following:
* It periodically polls the `tsv` table for `(event_name, version)` pairs, and compares
them to its table version cache. If it discovers a version that is higher than
the one in its cache (or it isn't in the cache), that table needs to be
migrated.
* Then it hits blueprint's `/migration` endpoint to discover the operations it needs to apply
to reach the next version. Example of the endpoint:
```
GET http://<blueprint>/migration/minute-watched?to_version=1
response body:
[
    {"Action":"add","Name":"time","ActionMetadata":{"column_options":" sortkey","column_type":"f@timestamp@unix","inbound":"time"}
    {"Action":"add","Name":"browser","ActionMetadata":{"column_options":"(180)","column_type":"varchar","inbound":"browser"}
    {"Action":"add","Name":"channel","ActionMetadata":{"column_options":"(25)","column_type":"varchar","inbound":"channel"}
    ...
]
```
* It then runs the `CREATE TABLE` or `ALTER` query and updates `infra.table_version`
in a transaction, and updates its local cache. It then moves on to the next migration.

The migrator also handles calls to the `/control/increment_version/:id` endpoint (see below).
It handles the necessary updates to `infra.table_version` and the in-memory version cache so that
only one goroutine is ever modifying them.

## Control

The control module provides an API to control aspects parts of the ingester, called from blueprint.

On error, each of these endpoints returns a 4xx or 5xx and a JSON object: {"Error": <a human readable string>}

POST endpoints:
* `/control/force_load`: Execute a force load. On success, response is empty with 204 (no content) status code.
Body of request must be JSON with:

```
    Table: name of the table to load
    Requester: name of the person or system requesting the load
```

* `/control/increment_version/:id`: Increment a table's version without waiting for a TSV to
come in and the migration to be executed. On success, response is empty with 204 (no content) status code.

GET endpoints:
* `/control/table_exists/:id`: Return if a table exists in the `infra.table_versions` table.
Can return false positives for tables that have been dropped.

Response format:

    {"Exists": bool}


### Blueprint's usage
Blueprint's UI forwards to the force load endpoint in response to a button press, and uses increment version
to drop tables which don't have any events being sent.

## License
[see LICENSE](LICENSE)
