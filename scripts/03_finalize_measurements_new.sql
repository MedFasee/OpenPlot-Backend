\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';

CREATE UNIQUE INDEX measurements_new_pkey
ON openplot.measurements_new (pdc_pmu_id, signal_id, ts);

CREATE INDEX ix_measurements_new_signal_ts
ON openplot.measurements_new (signal_id, ts DESC);

CREATE INDEX ix_measurements_new_pdc_signal_ts
ON openplot.measurements_new (pdc_pmu_id, signal_id, ts DESC);

ALTER TABLE openplot.measurements_new
ADD CONSTRAINT measurements_new_pdc_pmu_fk
FOREIGN KEY (pdc_pmu_id)
REFERENCES openplot.pdc_pmu(pdc_pmu_id)
ON DELETE CASCADE
NOT VALID;

ALTER TABLE openplot.measurements_new
ADD CONSTRAINT measurements_new_signal_fk
FOREIGN KEY (signal_id)
REFERENCES openplot.signal(signal_id)
ON DELETE CASCADE
NOT VALID;

ALTER TABLE openplot.measurements_new
VALIDATE CONSTRAINT measurements_new_pdc_pmu_fk;

ALTER TABLE openplot.measurements_new
VALIDATE CONSTRAINT measurements_new_signal_fk;

ANALYZE openplot.measurements_new;