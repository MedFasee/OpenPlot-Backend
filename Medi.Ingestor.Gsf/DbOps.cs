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

CREATE TABLE IF NOT EXISTS openplot.measurements (
  ts        timestamptz      NOT NULL,
  pdc_pmu_id int             NOT NULL REFERENCES openplot.pdc_pmu(pdc_pmu_id) ON DELETE CASCADE,
  signal_id int              NOT NULL REFERENCES openplot.signal(signal_id) ON DELETE CASCADE,
  value     double precision NOT NULL,
  PRIMARY KEY (signal_id, ts)
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
  created_at   timestamptz  NOT NULL DEFAULT now()
);

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
