using System;
using Npgsql;

internal static class DbOps
{
    public static void ValidateRequiredSchema(NpgsqlConnection conn)
    {
        using var cmd = new NpgsqlCommand(@"
            SELECT
                to_regclass('openplot.signal_points')      AS signal_points,
                to_regclass('openplot.measurements')      AS measurements,
                to_regclass('openplot.search_runs')       AS search_runs,
                to_regclass('openplot.ingest_chunks')     AS ingest_chunks;
        ", conn);

        using var reader = cmd.ExecuteReader();

        if (!reader.Read())
            throw new InvalidOperationException(
                "Não foi possível verificar as tabelas do schema openplot.");

        string[] tabelas =
        {
            "signal_points",
            "measurements",
            "search_runs",
            "ingest_chunks"
        };

        for (int i = 0; i < tabelas.Length; i++)
        {
            if (reader.IsDBNull(i))
            {
                throw new InvalidOperationException(
                    $"A tabela openplot.{tabelas[i]} não existe no banco.");
            }
        }
    }

    public static void UpdateStatus(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        Guid id,
        string status,
        int progress,
        string message)
    {
        using var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status = @s,
                   progress = @p,
                   message = @m
             WHERE id = @id
               AND LOWER(status) = 'running';
        ", conn, tx);

        cmd.Parameters.AddWithValue("s", status);
        cmd.Parameters.AddWithValue("p", progress);
        cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
        cmd.Parameters.AddWithValue("id", id);

        cmd.ExecuteNonQuery();
    }

    public static void MarkCanceled(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        Guid id,
        string message)
    {
        using var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status = 'canceled',
                   message = @m,
                   finished_at = now()
             WHERE id = @id
               AND LOWER(status) <> 'canceled';
        ", conn, tx);

        cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
        cmd.Parameters.AddWithValue("id", id);

        cmd.ExecuteNonQuery();
    }

    public static void MarkStarted(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        Guid id,
        string status,
        int progress,
        string message)
    {
        using var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status = @s,
                   progress = @p,
                   message = @m,
                   started_at = now(),
                   finished_at = NULL
             WHERE id = @id;
        ", conn, tx);

        cmd.Parameters.AddWithValue("s", status);
        cmd.Parameters.AddWithValue("p", progress);
        cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
        cmd.Parameters.AddWithValue("id", id);

        cmd.ExecuteNonQuery();
    }

    public static void MarkFinished(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        Guid id,
        string status,
        int progress,
        string message)
    {
        using var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET status = @s,
                   progress = @p,
                   message = @m,
                   finished_at = now()
             WHERE id = @id;
        ", conn, tx);

        cmd.Parameters.AddWithValue("s", status);
        cmd.Parameters.AddWithValue("p", progress);
        cmd.Parameters.AddWithValue("m", (object)message ?? DBNull.Value);
        cmd.Parameters.AddWithValue("id", id);

        cmd.ExecuteNonQuery();
    }
}