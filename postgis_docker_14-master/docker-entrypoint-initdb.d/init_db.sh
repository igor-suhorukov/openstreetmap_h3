#!/bin/bash

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"
echo "init OSM database"
"${psql[@]}" <<- 'EOSQL'
\i /input/static/database_init.sql
EOSQL

echo "parallel OSM data loading"
#https://github.com/docker-library/postgres/blob/master/docker-entrypoint.sh

find /input/sql/ | grep "/.*sql$" | sort | PGHOST= PGHOSTADDR=  parallel psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --no-password --no-psqlrc  --dbname "osmworld" -f

echo "finish OSM database initialization"

"${psql[@]}" <<- 'EOSQL'
\i /input/static/database_after_init.sql
EOSQL

