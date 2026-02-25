using Dapper;

namespace OpenPlot.ExportWorker.Data;

public sealed class RunComtradeRepo
{
    private readonly Db _db;
    public RunComtradeRepo(Db db) => _db = db;

    // dequeue: pega 1 queued e marca running
    public async Task<Guid?> TryDequeueAsync(CancellationToken ct)
    {
        const string sql = """
        WITH next_job AS (
          SELECT run_id
          FROM openplot.comtrade_runs
          WHERE status = 'queued'
          ORDER BY created_at
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        UPDATE openplot.comtrade_runs j
        SET status = 'running',
            started_at = now(),
            progress = 1,
            message = 'Iniciando...',
            error = NULL
        FROM next_job
        WHERE j.run_id = next_job.run_id
        RETURNING j.run_id;
        """;

        return await _db.Conn.QueryFirstOrDefaultAsync<Guid?>(new CommandDefinition(sql, cancellationToken: ct));
    }

    public Task UpdateProgressAsync(Guid runId, int progress, string? message, CancellationToken ct)
    {
        const string sql = """
        UPDATE openplot.comtrade_runs
        SET progress = @progress,
            message = @message
        WHERE run_id = @runId;
        """;
        return _db.Conn.ExecuteAsync(new CommandDefinition(sql, new { runId, progress, message }, cancellationToken: ct));
    }

    public Task MarkDoneAsync(Guid runId, string dirPath, string fileName, long sizeBytes, string sha256, CancellationToken ct)
    {
        const string sql = """
        UPDATE openplot.comtrade_runs
        SET status = 'done',
            progress = 100,
            message = 'Concluído',
            dir_path = @dirPath,
            file_name = @fileName,
            size_bytes = @sizeBytes,
            sha256 = @sha256,
            finished_at = now()
        WHERE run_id = @runId;
        """;
        return _db.Conn.ExecuteAsync(new CommandDefinition(sql, new { runId, dirPath, fileName, sizeBytes, sha256 }, cancellationToken: ct));
    }

    public Task MarkFailedAsync(Guid runId, string error, CancellationToken ct)
    {
        const string sql = """
        UPDATE openplot.comtrade_runs
        SET status = 'failed',
            message = 'Falha',
            error = @error,
            finished_at = now()
        WHERE run_id = @runId;
        """;
        return _db.Conn.ExecuteAsync(new CommandDefinition(sql, new { runId, error }, cancellationToken: ct));
    }
}