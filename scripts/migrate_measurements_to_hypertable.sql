\set ON_ERROR_STOP on
\timing on

-- Migração full de openplot.measurements para TimescaleDB hypertable.
-- Preserva a tabela atual como backup lógico: openplot.measurements_before_hypertable.
-- Executar em janela controlada para evitar writes concorrentes durante a troca.

SET statement_timeout = 0;
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = 0;

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE SCHEMA IF NOT EXISTS openplot;

BEGIN;

SELECT pg_advisory_xact_lock(hashtext('openplot.measurements.hypertable.migration'));

DO $$
BEGIN
	IF to_regclass('openplot.measurements') IS NULL THEN
		RAISE EXCEPTION 'Tabela openplot.measurements não encontrada.';
	END IF;

	IF EXISTS (
		SELECT 1
		FROM timescaledb_information.hypertables
		WHERE hypertable_schema = 'openplot'
		  AND hypertable_name = 'measurements'
	) THEN
		RAISE EXCEPTION 'A tabela openplot.measurements já é hypertable.';
	END IF;

	IF to_regclass('openplot.measurements_before_hypertable') IS NOT NULL THEN
		RAISE EXCEPTION 'Backup openplot.measurements_before_hypertable já existe. Revise antes de executar novamente.';
	END IF;
END
$$;

LOCK TABLE openplot.measurements IN ACCESS EXCLUSIVE MODE;

ALTER TABLE openplot.measurements RENAME TO measurements_before_hypertable;

CREATE TABLE openplot.measurements (
	ts timestamptz NOT NULL,
	pdc_pmu_id int NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
	signal_id int NOT NULL REFERENCES openplot.signal(signal_id) ON DELETE CASCADE,
	value double precision NOT NULL,
	PRIMARY KEY (pdc_pmu_id, signal_id, ts)
);

SELECT create_hypertable(
	'openplot.measurements',
	'ts',
	chunk_time_interval => INTERVAL '1 day',
	if_not_exists => TRUE,
	migrate_data => FALSE
);

INSERT INTO openplot.measurements (ts, pdc_pmu_id, signal_id, value)
SELECT ts, pdc_pmu_id, signal_id, value
FROM openplot.measurements_before_hypertable;

CREATE INDEX IF NOT EXISTS ix_measurements_signal_ts
	ON openplot.measurements (signal_id, ts DESC);

CREATE INDEX IF NOT EXISTS ix_measurements_pdc_signal_ts
	ON openplot.measurements (pdc_pmu_id, signal_id, ts DESC);

ANALYZE openplot.measurements;
ANALYZE openplot.measurements_before_hypertable;

DO $$
DECLARE
	src_count bigint;
	dst_count bigint;
BEGIN
	SELECT count(*) INTO src_count FROM openplot.measurements_before_hypertable;
	SELECT count(*) INTO dst_count FROM openplot.measurements;

	IF src_count <> dst_count THEN
		RAISE EXCEPTION 'Contagem divergente após migração. Origem: %, destino: %', src_count, dst_count;
	END IF;

	RAISE NOTICE 'Migração concluída com sucesso. Registros migrados: %', dst_count;
END
$$;

SELECT
	'openplot.measurements_before_hypertable' AS table_name,
	count(*) AS total_rows
FROM openplot.measurements_before_hypertable
UNION ALL
SELECT
	'openplot.measurements' AS table_name,
	count(*) AS total_rows
FROM openplot.measurements;

COMMIT;
