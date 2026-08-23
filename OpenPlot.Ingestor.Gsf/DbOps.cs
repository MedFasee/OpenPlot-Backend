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

CREATE TABLE IF NOT EXISTS openplot.measurements_wide_2 (
  ts             timestamptz      NOT NULL,
  pdc_pmu_id     int              NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  quality        integer          NULL,

  va_mod_v       double precision NULL,
  va_ang_deg     double precision NULL,
  vb_mod_v       double precision NULL,
  vb_ang_deg     double precision NULL,
  vc_mod_v       double precision NULL,
  vc_ang_deg     double precision NULL,

  ia_mod_a       double precision NULL,
  ia_ang_deg     double precision NULL,
  ib_mod_a       double precision NULL,
  ib_ang_deg     double precision NULL,
  ic_mod_a       double precision NULL,
  ic_ang_deg     double precision NULL,

  cthd_a_pct     double precision NULL,
  cthd_b_pct     double precision NULL,
  cthd_c_pct     double precision NULL,

  vthd_a_pct     double precision NULL,
  vthd_b_pct     double precision NULL,
  vthd_c_pct     double precision NULL,

  frequency_hz   double precision NULL,
  delta_freq_hz  double precision NULL,
  cfds_dig           double precision NULL,

  PRIMARY KEY (pdc_pmu_id, ts)
);

CREATE TABLE IF NOT EXISTS openplot.measurements_raw (
  ts        timestamptz      NOT NULL,
  pdc_pmu_id int             NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  point_id  bigint           NOT NULL,
  value     double precision NOT NULL,
  PRIMARY KEY (pdc_id, point_id, ts)
);

-- fila / jobs
CREATE TABLE IF NOT EXISTS openplot.search_runs (
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
  created_at   timestamptz  NOT NULL DEFAULT now(),
  started_at   timestamptz  NULL,
  finished_at     timestamptz  NULL
);

ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS started_at timestamptz NULL;
ALTER TABLE openplot.search_runs ADD COLUMN IF NOT EXISTS finished_at timestamptz NULL;
ALTER TABLE openplot.measurements_wide_2 ADD COLUMN IF NOT EXISTS quality integer NULL;

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
