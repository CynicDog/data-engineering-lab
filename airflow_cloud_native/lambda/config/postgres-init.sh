#!/usr/bin/env bash
set -euo pipefail

# Creates a second database alongside `airflow` for the serving warehouse,
# owned by a dedicated user.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE USER ${WAREHOUSE_USER} WITH PASSWORD '${WAREHOUSE_PASSWORD}';
    CREATE DATABASE ${WAREHOUSE_DB} OWNER ${WAREHOUSE_USER};
    GRANT ALL PRIVILEGES ON DATABASE ${WAREHOUSE_DB} TO ${WAREHOUSE_USER};
SQL
