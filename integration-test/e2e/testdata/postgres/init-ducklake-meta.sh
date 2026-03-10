#!/bin/bash
# Create a separate database for DuckLake metadata.
# Executed by PostgreSQL entrypoint alongside init.sql.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ducklake_meta OWNER $POSTGRES_USER;
EOSQL
