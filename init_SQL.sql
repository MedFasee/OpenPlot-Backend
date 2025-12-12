CREATE SCHEMA IF NOT EXISTS openPlot;

-- 0) Domínios/enums úteis
CREATE TYPE qty_kind AS ENUM ('Voltage','Current','Frequency');
CREATE TYPE phase_kind AS ENUM ('A','B','C','None');      -- None para freq/dfreq/digital
CREATE TYPE comp_kind AS ENUM ('MAG','ANG','FREQ','DFREQ');

-- 1) Fonte de dados (PDC / historian)
CREATE TABLE openPlot.pdc (
  pdc_id       SERIAL PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,        -- ex: openHistorian_214 / OH2_ONS_RJ_XRTE_60fps
  kind         TEXT NOT NULL,               -- ex: openpdc / openhistorian2
  fps          INTEGER NOT NULL,            -- 60, 120
  address      TEXT NOT NULL,               -- host:port
  user_name    TEXT,
  password     TEXT
);

-- 2) PMU 
CREATE TABLE openPlot.pmu (
  pmu_id       SERIAL PRIMARY KEY,
  id_name      TEXT NOT NULL,               -- ex: "UFPA-PMU_Belem_PA"
  full_name    TEXT,
  volt_level   INTEGER,                     -- em volts
  area         TEXT,                        -- N, NE, SE, S, CO...
  state        TEXT,                        -- UF
  station      TEXT,
  lat          DOUBLE PRECISION,
  lon          DOUBLE PRECISION,
  UNIQUE (id_name)
);

-- 3) Associação PDC x PMU (a mesma PMU pode existir em vários PDCs)
CREATE TABLE openPlot.pdc_pmu (
  pdc_pmu_id   SERIAL PRIMARY KEY,
  pdc_id       INTEGER NOT NULL REFERENCES openPlot.pdc(pdc_id) ON DELETE CASCADE,
  pmu_id       INTEGER NOT NULL REFERENCES openPlot.pmu(pmu_id) ON DELETE CASCADE,
  -- se o mesmo PDC exporta a mesma PMU mais de uma vez com apelidos diferentes,
  -- guardar o "idName" que veio daquele arquivo aqui também:
  pdc_local_id TEXT,
  UNIQUE (pdc_id, pmu_id)
);


-- 4) Signals (cada linha = 1 PPA)
--    Para fasor viram DUAS linhas: MAG e ANG. Para frequência e dfreq idem (component FREQ/DFREQ).
CREATE TABLE openPlot.signal (
  signal_id        SERIAL PRIMARY KEY,
  pdc_pmu_id       INTEGER NOT NULL REFERENCES openPlot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  name             TEXT NOT NULL,           -- pName/fName/dfName do XML (ex: TENSAO_A, FREQUENCIA)
  quantity         qty_kind NOT NULL,       -- Voltage|Current|Frequency
  phase            phase_kind NOT NULL,     -- A,B,C,None
  component        comp_kind NOT NULL,      -- MAG/ANG/FREQ/DFREQ
  historian_point  INTEGER NOT NULL,        -- modId/angId/fId/dfId

  UNIQUE (pdc_pmu_id, name, phase, component)
);

-- Índices
CREATE INDEX idx_signal_point ON openPlot.signal(historian_point);
CREATE INDEX idx_signal_lookup ON openPlot.signal(pdc_pmu_id, quantity, phase, component);

-- 5) 
CREATE TABLE IF NOT EXISTS openPlot.signal_points (
  signal_id  int     NOT NULL REFERENCES openPlot.signal(signal_id) ON DELETE CASCADE,
  pdc_id     int     NOT NULL REFERENCES openPlot.pdc(pdc_id)     ON DELETE CASCADE,
  role       text    NOT NULL,  -- 'mod'|'ang'|'value'
  point_id   bigint  NOT NULL,
  UNIQUE (pdc_id, point_id),
  UNIQUE (signal_id, role)
);

-- 6) 
CREATE TABLE IF NOT EXISTS openPlot.measurements (
  ts        timestamptz      NOT NULL,
  pdc_pmu_id int             NOT NULL REFERENCES openPlot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  signal_id int              NOT NULL REFERENCES openPlot.signal(signal_id) ON DELETE CASCADE,
  value     double precision NOT NULL,
  PRIMARY KEY (signal_id, ts)
);

-- 7) 
CREATE TABLE IF NOT EXISTS openplot.measurements (
    ts           timestamptz      NOT NULL,
    pdc_pmu_id   int              NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
    signal_id    int              NOT NULL REFERENCES openplot.signal(signal_id) ON DELETE CASCADE,
    value        double precision NOT NULL,

    -- NOVA PK, correta
    PRIMARY KEY (pdc_pmu_id, signal_id, ts)
);

-- 8) -- fila / jobs
CREATE TABLE IF NOT EXISTS openPlot.search_runs (
  id           uuid         PRIMARY KEY,
  source       text         NOT NULL,
  terminal_id  text         NULL,
  signals      jsonb        NOT NULL,
  from_ts      timestamptz  NOT NULL,
  to_ts        timestamptz  NOT NULL,
  select_rate  int          NOT NULL DEFAULT 0,
  status       text         NOT NULL,
  progress     int          NOT NULL DEFAULT 0,
  message      text         NULL,
  created_at   timestamptz  NOT NULL DEFAULT now()
);

-- 9) 
CREATE TABLE IF NOT EXISTS openPlot.ingest_chunks (
  id          bigserial      PRIMARY KEY,
  job_id      uuid           NOT NULL REFERENCES openPlot.search_runs(id) ON DELETE CASCADE,
  signal_id   int            NOT NULL REFERENCES openPlot.signal(signal_id) ON DELETE CASCADE,
  from_ts     timestamptz    NOT NULL,
  to_ts       timestamptz    NOT NULL,
  rowcount    int            NOT NULL,
  inserted_at timestamptz    NOT NULL DEFAULT now()
);


-- 10)
ALTER TABLE openplot.search_runs
    ADD COLUMN IF NOT EXISTS pdc_id integer,
    ADD COLUMN IF NOT EXISTS signal_count integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pmu_count integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS label text,
    ADD COLUMN IF NOT EXISTS pmus jsonb;

-- 11) Lógica THD
ALTER TYPE comp_kind ADD VALUE IF NOT EXISTS 'THD';

-- 12) PMUs que retornam dados
ALTER TABLE openplot.search_runs
ADD COLUMN pmus_ok jsonb;