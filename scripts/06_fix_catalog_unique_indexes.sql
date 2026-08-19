\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';

-- Diagnóstico de possíveis duplicidades que impedem índice único
SELECT 'openplot.pdc(name)' AS target, name, COUNT(*) AS qty
FROM openplot.pdc
GROUP BY name
HAVING COUNT(*) > 1;

SELECT 'openplot.pmu(id_name)' AS target, id_name, COUNT(*) AS qty
FROM openplot.pmu
GROUP BY id_name
HAVING COUNT(*) > 1;

SELECT 'openplot.pdc_pmu(pdc_id, pmu_id)' AS target, pdc_id, pmu_id, COUNT(*) AS qty
FROM openplot.pdc_pmu
GROUP BY pdc_id, pmu_id
HAVING COUNT(*) > 1;

SELECT 'openplot.signal(pdc_pmu_id, name, phase, component)' AS target,
	   pdc_pmu_id, name, phase, component, COUNT(*) AS qty
FROM openplot.signal
GROUP BY pdc_pmu_id, name, phase, component
HAVING COUNT(*) > 1;

-- Se as consultas acima retornarem linhas, saneie duplicidades antes de seguir.
CREATE UNIQUE INDEX IF NOT EXISTS ux_pdc_name
ON openplot.pdc (name);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pmu_id_name
ON openplot.pmu (id_name);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pdc_pmu_pdc_id_pmu_id
ON openplot.pdc_pmu (pdc_id, pmu_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_pdc_pmu_name_phase_component
ON openplot.signal (pdc_pmu_id, name, phase, component);
