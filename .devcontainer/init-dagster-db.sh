#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster') THEN
            CREATE DATABASE dagster;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'calibrate') THEN
            CREATE DATABASE calibrate;
        END IF;
    END
    \$\$;
EOSQL