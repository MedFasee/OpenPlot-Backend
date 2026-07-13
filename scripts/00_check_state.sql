\set ON_ERROR_STOP on
\timing on

SELECT current_database(), current_schema();

SELECT
    to_regclass('openplot.measurements') AS measurements,
    to_regclass('openplot.measurements_new') AS measurements_new,
    to_regclass('openplot.measurements_before_hypertable') AS measurements_backup;

SELECT extname, extversion
FROM pg_extension
WHERE extname = 'timescaledb';

SELECT
    pg_size_pretty(pg_relation_size('openplot.measurements')) AS somente_dados,
    pg_size_pretty(pg_total_relation_size('openplot.measurements')) AS dados_mais_indices;