\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';

BEGIN;

DELETE FROM openplot.measurements_new
WHERE ts >= :'from_utc'::timestamptz
  AND ts <  :'to_utc'::timestamptz;

INSERT INTO openplot.measurements_new (ts, pdc_pmu_id, signal_id, value)
SELECT ts, pdc_pmu_id, signal_id, value
FROM openplot.measurements
WHERE ts >= :'from_utc'::timestamptz
  AND ts <  :'to_utc'::timestamptz;

COMMIT;