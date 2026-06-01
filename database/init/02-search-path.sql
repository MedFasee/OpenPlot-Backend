ALTER DATABASE postgres SET search_path TO openplot, public;

ALTER ROLE postgres SET search_path TO openplot, public;

ALTER ROLE postgres IN DATABASE postgres SET search_path TO openplot, public;


CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_pdc_pmu_name_phase_component
ON openplot.signal (pdc_pmu_id, name, phase, component);