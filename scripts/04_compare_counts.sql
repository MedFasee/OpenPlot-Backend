\set ON_ERROR_STOP on
\timing on

SELECT 'measurements_original' AS tabela, count(*) AS total
FROM openplot.measurements
UNION ALL
SELECT 'measurements_new' AS tabela, count(*) AS total
FROM openplot.measurements_new;