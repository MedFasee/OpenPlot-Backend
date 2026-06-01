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

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set ON_ERROR_STOP=1 <<'SQL'
CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_pdc_pmu_name_phase_component
ON openplot.signal (pdc_pmu_id, name, phase, component);
SQL

echo "Restore concluído."