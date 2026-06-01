using System;
using Npgsql;

internal static class DbOps
{
    public static void EnsureSchema(NpgsqlConnection conn)
    {
        using (var cmd = new NpgsqlCommand(@"
CREATE SCHEMA IF NOT EXISTS openplot;

-- dimensões (assumo que você já tem pdc/pmu/signals criadas via importador)

CREATE TABLE IF NOT EXISTS openplot.signal_points (
  signal_id  int     NOT NULL REFERENCES openplot.signal(signal_id) ON DELETE CASCADE,
  pdc_id     int     NOT NULL REFERENCES openplot.pdc(pdc_id)     ON DELETE CASCADE,
  role       text    NOT NULL,  -- 'mod'|'ang'|'value'
  point_id   bigint  NOT NULL,
  UNIQUE (pdc_id, point_id),
  UNIQUE (signal_id, role)
);

CREATE TABLE IF NOT EXISTS openplot.measurements_ht (
  ts        timestamptz      NOT NULL,
  pdc_pmu_id int             NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  signal_id int              NOT NULL REFERENCES openplot.signal(signal_id) ON DELETE CASCADE,
  value     double precision NOT NULL,
  PRIMARY KEY (pdc_pmu_id, signal_id, ts)
);

CREATE TABLE IF NOT EXISTS openplot.measurements_raw (
  ts        timestamptz      NOT NULL,
  pdc_pmu_id int             NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  point_id  bigint           NOT NULL,
  value     double precision NOT NULL,
  PRIMARY KEY (pdc_pmu_id, point_id, ts)
);

CREATE OR REPLACE VIEW openplot.measurements AS
SELECT ts, pdc_pmu_id, signal_id, value
  FROM openplot.measurements_ht;

-- fila / jobs
CREATE TABLE IF NOT EXISTS openplot.search_runs (
  id           uuid         PRIMARY KEY,
  source       text         NOT NULL,
  terminal_id  text         NULL,
  pmus         jsonb        NULL,
  signals      jsonb        NOT NULL,
  from_ts      timestamptz  NOT NULL,
  to_ts        timestamptz  NOT NULL,
  select_rate  int          NOT NULL DEFAULT 0,
  status       text         NOT NULL DEFAULT 'queued',
  progress     int          NOT NULL DEFAULT 0,
  message      text         NULL,
  created_at   timestamptz  NOT NULL DEFAULT now(),
  pdc_id       int          NULL REFERENCES openplot.pdc(pdc_id) ON DELETE SET NULL,
  signal_count int          NULL DEFAULT 0,
  pmu_count    int          NULL DEFAULT 0,
  label        text         NULL,
  pmus_ok      jsonb        NULL,
  username     text         NULL,
  shared       boolean      NOT NULL DEFAULT false,
  is_visible   boolean      NOT NULL DEFAULT true,
  deleted_at   timestamptz  NULL,
  started_at   timestamptz  NULL,
  finished_at  timestamptz  NULL
);

ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS pmus jsonb NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS pdc_id int NULL REFERENCES openplot.pdc(pdc_id) ON DELETE SET NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS signal_count int NULL DEFAULT 0;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS pmu_count int NULL DEFAULT 0;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS label text NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS pmus_ok jsonb NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS username text NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS shared boolean NOT NULL DEFAULT false;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS is_visible boolean NOT NULL DEFAULT true;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS deleted_at timestamptz NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS started_at timestamptz NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS finished_at timestamptz NULL;

UPDATE openplot.search_runs
   SET status = CASE LOWER(status)
       WHEN 'done' THEN 'completed'
       WHEN 'no_data' THEN 'completed'
       WHEN 'bad_connection' THEN 'failed'
       ELSE LOWER(status)
   END
 WHERE LOWER(status) IN ('queued', 'running', 'completed', 'failed', 'canceled', 'done', 'no_data', 'bad_connection');

CREATE TABLE IF NOT EXISTS openplot.ingest_chunks (
  id          bigserial      PRIMARY KEY,
  job_id      uuid           NOT NULL REFERENCES openplot.search_runs(id) ON DELETE CASCADE,
  signal_id   int            NOT NULL REFERENCES openplot.signal(signal_id) ON DELETE CASCADE,
  from_ts     timestamptz    NOT NULL,
  to_ts       timestamptz    NOT NULL,
  rowcount    int            NOT NULL,
  inserted_at timestamptz    NOT NULL DEFAULT now()
);
", conn))
        {
            cmd.ExecuteNonQuery();
        }
    }

    public static void UpdateStatus(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, string status, int progress, string message)
    {
        using (var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status=@s, progress=@p, message=@m
             WHERE id=@id
               AND LOWER(status) = 'running';", conn, tx))
        {
            cmd.Parameters.AddWithValue("s", status);
            cmd.Parameters.AddWithValue("p", progress);
            cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
            cmd.Parameters.AddWithValue("id", id);
            cmd.ExecuteNonQuery();
        }
    }

    public static void MarkCanceled(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, string message)
    {
        using (var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status='canceled',
                   message=@m,
                   finished_at=now()
             WHERE id=@id
               AND LOWER(status) <> 'canceled';", conn, tx))
        {
            cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
            cmd.Parameters.AddWithValue("id", id);
            cmd.ExecuteNonQuery();
        }
    }

    public static void MarkStarted(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, string status, int progress, string message)
    {
        using (var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status=@s,
                   progress=@p,
                   message=@m,
                   started_at=now(),
                   finished_at=NULL
             WHERE id=@id;", conn, tx))
        {
            cmd.Parameters.AddWithValue("s", status);
            cmd.Parameters.AddWithValue("p", progress);
            cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
            cmd.Parameters.AddWithValue("id", id);
            cmd.ExecuteNonQuery();
        }
    }

    public static void MarkFinished(NpgsqlConnection conn, NpgsqlTransaction tx, Guid id, string status, int progress, string message)
    {
        using (var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status=@s,
                   progress=@p,
                   message=@m,
                   finished_at=now()
             WHERE id=@id;", conn, tx))
        {
            cmd.Parameters.AddWithValue("s", status);
            cmd.Parameters.AddWithValue("p", progress);
            cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
            cmd.Parameters.AddWithValue("id", id);
            cmd.ExecuteNonQuery();
        }
    }
}
