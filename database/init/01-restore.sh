#!/bin/sh
set -e

echo "Restaurando dump do OpenPlot..."

pg_restore \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --verbose \
  --no-owner \
  --no-privileges \
  /dumps/openplot.dump

echo "Restore concluído."