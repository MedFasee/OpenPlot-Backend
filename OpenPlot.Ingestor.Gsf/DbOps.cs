using System;
using Npgsql;

namespace OpenPlot.Ingestor.Gsf
{
    internal static class DbOps
    {
        public static void EnsureSchema(NpgsqlConnection conn)
        {
            using (var cmd = new NpgsqlCommand(@"
                CREATE SCHEMA IF NOT EXISTS medi;

                CREATE TABLE IF NOT EXISTS medi.measurements (
                    ts            timestamptz NOT NULL,
                    signal_id     text        NOT NULL,
                    historian_id  integer     NOT NULL,
                    value         double precision NOT NULL,
                    region        text NULL,
                    substation    text NULL,
                    CONSTRAINT uq_measure UNIQUE (signal_id, historian_id, ts)
                );

                CREATE TABLE IF NOT EXISTS medi.measurements_stage (
                    ts            timestamptz NOT NULL,
                    signal_id     text        NOT NULL,
                    historian_id  integer     NOT NULL,
                    value         double precision NOT NULL,
                    region        text NULL,
                    substation    text NULL
                );

                CREATE INDEX IF NOT EXISTS ix_meas_signal_hid_ts
                  ON medi.measurements (signal_id, historian_id, ts);
                CREATE INDEX IF NOT EXISTS ix_meas_ts
                  ON medi.measurements (ts);
            ", conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        public static void UpdateStatus(
            NpgsqlConnection conn, NpgsqlTransaction tx,
            Guid id, string status, int progress, string message)
        {
            using (var cmd = new NpgsqlCommand(@"
                UPDATE medi.search_runs
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
}
