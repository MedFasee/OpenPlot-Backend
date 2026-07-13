\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'measurements_new_pdc_pmu_fk'
          AND conrelid = 'openplot.measurements_new'::regclass
    ) THEN
        ALTER TABLE openplot.measurements_new
        ADD CONSTRAINT measurements_new_pdc_pmu_fk
        FOREIGN KEY (pdc_pmu_id)
        REFERENCES openplot.pdc_pmu(pdc_pmu_id)
        ON DELETE CASCADE
        NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'measurements_new_signal_fk'
          AND conrelid = 'openplot.measurements_new'::regclass
    ) THEN
        ALTER TABLE openplot.measurements_new
        ADD CONSTRAINT measurements_new_signal_fk
        FOREIGN KEY (signal_id)
        REFERENCES openplot.signal(signal_id)
        ON DELETE CASCADE
        NOT VALID;
    END IF;
END
$$;

ALTER TABLE openplot.measurements_new
VALIDATE CONSTRAINT measurements_new_pdc_pmu_fk;

ALTER TABLE openplot.measurements_new
VALIDATE CONSTRAINT measurements_new_signal_fk;

ANALYZE openplot.measurements_new;