-- garante que o schema existe
CREATE SCHEMA IF NOT EXISTS openplot;

-- cria a tabela dentro do schema
CREATE TABLE openplot.api_request_log (
    id              BIGSERIAL       PRIMARY KEY,
    timestamp_utc   TIMESTAMPTZ(6)  NOT NULL,
    method          VARCHAR(10)     NOT NULL,
    path            VARCHAR(512)    NOT NULL,
    status_code     INT             NOT NULL,
    elapsed_ms      INT             NOT NULL,
    user_name       VARCHAR(255)    NULL,
    user_id         VARCHAR(255)    NULL,
    ip              VARCHAR(45)     NULL,
    correlation_id  VARCHAR(64)     NULL,
    user_agent      TEXT            NULL
);

-- índices de desempenho
CREATE INDEX idx_api_request_log_timestamp 
    ON openplot.api_request_log (timestamp_utc);

CREATE INDEX idx_api_request_log_path 
    ON openplot.api_request_log (path);

CREATE INDEX idx_api_request_log_user 
    ON openplot.api_request_log (user_name);

CREATE INDEX idx_api_request_log_corr 
    ON openplot.api_request_log (correlation_id);


ALTER TABLE openplot.api_request_log
ADD COLUMN protocol       VARCHAR(16),
ADD COLUMN content_type   TEXT,
ADD COLUMN content_length BIGINT;

ALTER TABLE openplot.api_request_log
ADD COLUMN request_body TEXT;

ALTER TABLE openplot.api_request_log
ADD COLUMN IF NOT EXISTS query_string TEXT;


-- Adição de coluna usuário (dando dono a cada consulta)
ALTER TABLE openplot.search_runs
  ADD COLUMN username text;