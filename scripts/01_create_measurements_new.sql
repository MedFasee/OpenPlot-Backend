\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE SCHEMA IF NOT EXISTS openplot;

DO $$
BEGIN
    IF to_regclass('openplot.measurements') IS NULL THEN
        RAISE EXCEPTION 'Tabela openplot.measurements não encontrada.';
    END IF;

    IF to_regclass('openplot.measurements_new') IS NOT NULL THEN
        RAISE EXCEPTION 'Tabela openplot.measurements_new já existe. Revise antes de continuar.';
    END IF;
END
$$;

CREATE TABLE openplot.measurements_new (
    ts timestamptz NOT NULL,
    pdc_pmu_id int NOT NULL,
    signal_id int NOT NULL,
    value double precision NOT NULL
);

SELECT create_hypertable(
    'openplot.measurements_new',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => FALSE
);