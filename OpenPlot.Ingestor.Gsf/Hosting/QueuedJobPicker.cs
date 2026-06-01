using Npgsql;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal interface IQueuedJobPicker
{
    SearchRunJob? TryPickQueuedJob();
}

internal sealed class QueuedJobPicker : IQueuedJobPicker
{
    private readonly IngestorRuntimeContext _runtimeContext;

    public QueuedJobPicker(IngestorRuntimeContext runtimeContext)
    {
        _runtimeContext = runtimeContext;
    }

    public SearchRunJob? TryPickQueuedJob()
    {
        using var conn = new NpgsqlConnection(_runtimeContext.Options.PgConnString);
        conn.Open();

        using var tx = conn.BeginTransaction();
        const string pickSql = @"
            SELECT id, source, terminal_id, signals::text, from_ts, to_ts, select_rate, pmus::text
              FROM openplot.search_runs
             WHERE status = 'queued'
             ORDER BY created_at
             FOR UPDATE SKIP LOCKED
             LIMIT 1;";

        using var cmd = new NpgsqlCommand(pickSql, conn, tx);
        using var rdr = cmd.ExecuteReader();
        if (!rdr.Read())
        {
            rdr.Close();
            tx.Commit();
            return null;
        }

        var job = new SearchRunJob
        {
            Id = rdr.GetGuid(0),
            Source = rdr.GetString(1),
            TerminalId = rdr.IsDBNull(2) ? null : rdr.GetString(2),
            SignalsJson = rdr.GetString(3),
            From = rdr.GetDateTime(4),
            To = rdr.GetDateTime(5),
            SelectRate = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6),
            PmusJson = rdr.IsDBNull(7) ? null : rdr.GetString(7)
        };

        rdr.Close();
        DbOps.MarkStarted(conn, tx, job.Id, "running", 1, "Iniciando");
        tx.Commit();
        return job;
    }
}
