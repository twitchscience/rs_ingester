CREATE SCHEMA IF NOT EXISTS infra;

CREATE TABLE IF NOT EXISTS infra.table_version (
    name character varying(256),
    version integer,
    ts timestamp without time zone default GETDATE()
);
