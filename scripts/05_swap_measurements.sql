\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';

BEGIN;

LOCK TABLE openplot.measurements IN ACCESS EXCLUSIVE MODE;
LOCK TABLE openplot.measurements_new IN ACCESS EXCLUSIVE MODE;

ALTER TABLE openplot.measurements
RENAME TO measurements_before_hypertable;

-- Libera os nomes dos índices antigos.
ALTER INDEX IF EXISTS openplot.measurements_pkey
RENAME TO measurements_before_hypertable_pkey;

ALTER INDEX IF EXISTS openplot.ix_measurements_signal_ts
RENAME TO ix_measurements_before_hypertable_signal_ts;

ALTER INDEX IF EXISTS openplot.ix_measurements_pdc_signal_ts
RENAME TO ix_measurements_before_hypertable_pdc_signal_ts;

ALTER INDEX IF EXISTS openplot.ix_measurements_ts
RENAME TO ix_measurements_before_hypertable_ts;

ALTER INDEX IF EXISTS openplot.measurements_ts_idx
RENAME TO measurements_before_hypertable_ts_idx;

ALTER TABLE openplot.measurements_new
RENAME TO measurements;

-- Dá os nomes finais aos índices da nova hypertable.
ALTER INDEX IF EXISTS openplot.measurements_new_pkey
RENAME TO measurements_pkey;

ALTER INDEX IF EXISTS openplot.ix_measurements_new_signal_ts
RENAME TO ix_measurements_signal_ts;

ALTER INDEX IF EXISTS openplot.measurements_new_ts_idx
RENAME TO measurements_ts_idx;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'openplot.measurements'::regclass
          AND conname = 'measurements_new_pdc_pmu_fk'
    ) THEN
        ALTER TABLE openplot.measurements
        RENAME CONSTRAINT measurements_new_pdc_pmu_fk
        TO measurements_pdc_pmu_id_fkey;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'openplot.measurements'::regclass
          AND conname = 'measurements_new_signal_fk'
    ) THEN
        ALTER TABLE openplot.measurements
        RENAME CONSTRAINT measurements_new_signal_fk
        TO measurements_signal_id_fkey;
    END IF;
END
$$;

COMMIT;

ANALYZE openplot.measurements;