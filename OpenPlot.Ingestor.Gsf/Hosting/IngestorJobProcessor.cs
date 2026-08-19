using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using OpenPlot.Ingestor.Gsf.Repository;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal interface IIngestorJobProcessor
{
    void ProcessJob(SearchRunJob job, int workerId);
}

internal sealed class IngestorJobProcessor : IIngestorJobProcessor
{
    private readonly IngestorRuntimeContext _runtimeContext;
    private readonly IIngestorChunkPipeline _chunkPipeline;

    public IngestorJobProcessor(
        IngestorRuntimeContext runtimeContext,
        IIngestorChunkPipeline chunkPipeline)
    {
        _runtimeContext = runtimeContext;
        _chunkPipeline = chunkPipeline;
    }

    private string PgConnString => _runtimeContext.Options.PgConnString;
    private int ChunkMinutes => _runtimeContext.Options.ChunkMinutes;

    public void ProcessJob(SearchRunJob job, int workerId)
    {
        var jobSw = Stopwatch.StartNew();
        IngestorProgressReporter? progress = null;

        using var conn = new NpgsqlConnection(PgConnString);
        conn.Open();

        try
        {
            using (TimeBlock($"JOB {job.Id} (worker={workerId}, source={job.Source}) from={job.From:O} to={job.To:O}"))
            using (var watchdog = StartWatchdog(TimeSpan.FromMinutes(2), $"JOB {job.Id}"))
            {
                ThrowIfJobCancellationRequested(job.Id);

                var fromUtc = job.From.Kind == DateTimeKind.Utc ? job.From : job.From.ToUniversalTime();
                var toUtc = job.To.Kind == DateTimeKind.Utc ? job.To : job.To.ToUniversalTime();

                var sysCfg = DbSystemDataFactory.BuildByPdcName(
                    PgConnString,
                    job.Source,
                    TimeSpan.FromMinutes(10));

                var pmuList = TryParsePmus(job.PmusJson);
                var nPmus = pmuList is { Count: > 0 } ? pmuList.Count : 1;
                var nIntervals = CountIntervals(fromUtc, toUtc);
                progress = new IngestorProgressReporter(
                    PgConnString,
                    job.Id,
                    nPmus * nIntervals,
                    nIntervals);

                List<string>? pmusComDados = null;
                if (pmuList is { Count: > 0 })
                {
                    pmusComDados = new List<string>();

                    foreach (var pmuIdName in pmuList)
                    {
                        ThrowIfJobCancellationRequested(job.Id);

                        var pmuSw = Stopwatch.StartNew();
                        var reporterPmuName = pmuIdName;

                        try
                        {
                            var term = TerminalResolver.Resolve(sysCfg, pmuIdName);
                            reporterPmuName = term.Id;

                            var channels = _chunkPipeline.LoadChannels(job.Source ?? sysCfg.Name, pmuIdName);
                            if (channels.Count == 0)
                                throw new Exception("Nenhum canal encontrado no DB para a PMU '" + pmuIdName + "'.");

                            var teveDados = _chunkPipeline.FetchAndInsert(
                                job.Id,
                                job.Source ?? sysCfg.Name,
                                sysCfg,
                                term,
                                channels,
                                fromUtc,
                                toUtc,
                                job.SelectRate,
                                progress,
                                IsJobCancellationRequested,
                                id => new JobCanceledException(id));

                            if (teveDados)
                                pmusComDados.Add(pmuIdName);
                        }
                        finally
                        {
                            pmuSw.Stop();
                            progress.CompletePmu(reporterPmuName, pmuSw.Elapsed);
                        }
                    }
                }

                ThrowIfJobCancellationRequested(job.Id);

                jobSw.Stop();

                using var tx2 = conn.BeginTransaction();
                SavePmusOk(conn, tx2, job.Id, pmusComDados);

                if (pmusComDados is null || pmusComDados.Count == 0)
                {
                    var finalMessage = progress.BuildFinalMessage(
                        jobSw.Elapsed,
                        "no_data",
                        "Consulta executada com sucesso, porém sem dados no intervalo solicitado");

                    DbOps.MarkFinished(
                        conn,
                        tx2,
                        job.Id,
                        "no_data",
                        100,
                        finalMessage);
                }
                else
                {
                    var finalMessage = progress.BuildFinalMessage(
                        jobSw.Elapsed,
                        "done");

                    DbOps.MarkFinished(
                        conn,
                        tx2,
                        job.Id,
                        "done",
                        100,
                        finalMessage);
                }

                tx2.Commit();
                watchdog.Cancel();
            }
        }
        catch (JobCanceledException ex)
        {
            jobSw.Stop();
            Console.WriteLine("[cancelado] job " + job.Id + ": " + ex.Message);

            var message = progress?.BuildFinalMessage(
                jobSw.Elapsed,
                "canceled",
                "Cancelado pelo usuário")
                ?? "Cancelado pelo usuário";

            try
            {
                using var tx2 = conn.BeginTransaction();
                DbOps.MarkCanceled(conn, tx2, job.Id, message);
                tx2.Commit();
            }
            catch
            {
            }
        }
        catch (InvalidConnectionException ex)
        {
            jobSw.Stop();
            Console.WriteLine("[bad_connection] job " + job.Id + ": " + ex.Message);

            var message = progress?.BuildFinalMessage(
                jobSw.Elapsed,
                "bad_connection",
                ex.Message)
                ?? ("bad_connection: " + ex.Message);

            MarkBadConnection(
                conn,
                job.Id,
                progress?.CurrentProgressPercent ?? 0,
                message);
        }
        catch (Exception ex)
        {
            jobSw.Stop();
            Console.WriteLine("[erro] job " + job.Id + ": " + ex.Message);

            var message = progress?.BuildFinalMessage(
                jobSw.Elapsed,
                "failed",
                ex.Message)
                ?? ex.Message;

            try
            {
                using var tx2 = conn.BeginTransaction();
                DbOps.MarkFinished(
                    conn,
                    tx2,
                    job.Id,
                    "failed",
                    progress?.CurrentProgressPercent ?? 0,
                    message);
                tx2.Commit();
            }
            catch
            {
            }
        }
    }

    private bool IsJobCancellationRequested(Guid jobId)
    {
        using var conn = new NpgsqlConnection(PgConnString);
        conn.Open();

        using var cmd = new NpgsqlCommand(@"
            SELECT LOWER(status) = 'canceled'
              FROM openplot.search_runs
             WHERE id = @id;", conn);
        cmd.Parameters.AddWithValue("id", jobId);
        var value = cmd.ExecuteScalar();
        return value is bool canceled && canceled;
    }

    private void ThrowIfJobCancellationRequested(Guid jobId)
    {
        if (IsJobCancellationRequested(jobId))
            throw new JobCanceledException(jobId);
    }

    private int CountIntervals(DateTime fromUtc, DateTime toUtc)
    {
        var totalSpan = toUtc - fromUtc;
        if (totalSpan <= TimeSpan.Zero)
            return 1;

        var chunkSize = TimeSpan.FromMinutes(Math.Max(1, ChunkMinutes));
        if (chunkSize > totalSpan)
            chunkSize = totalSpan;

        var n = 0;
        for (var cs = fromUtc; cs < toUtc; cs = cs.Add(chunkSize))
            n++;

        return Math.Max(1, n);
    }

    private static void SavePmusOk(NpgsqlConnection conn, NpgsqlTransaction tx, Guid jobId, List<string>? pmusOk)
    {
        var normalized = (pmusOk ?? new List<string>())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var json = JsonSerializer.Serialize(normalized);

        using var cmd = new NpgsqlCommand(@"
            UPDATE openplot.search_runs
               SET pmus_ok = @pmus_ok::jsonb
             WHERE id = @id;", conn, tx);
        cmd.Parameters.AddWithValue("@id", jobId);
        cmd.Parameters.AddWithValue("@pmus_ok", json);
        cmd.ExecuteNonQuery();
    }

    private static void MarkBadConnection(
        NpgsqlConnection conn,
        Guid id,
        int progress,
        string message)
    {
        try
        {
            using var tx = conn.BeginTransaction();
            DbOps.MarkFinished(
                conn,
                tx,
                id,
                "bad_connection",
                progress,
                message);
            tx.Commit();
        }
        catch
        {
        }
    }

    private static string FmtMs(long ms)
    {
        if (ms < 1000)
            return ms + "ms";

        var ts = TimeSpan.FromMilliseconds(ms);
        if (ts.TotalMinutes >= 1)
            return $"{(int)ts.TotalMinutes}m{ts.Seconds:D2}s";

        return $"{ts.Seconds}s{ts.Milliseconds:D3}ms";
    }

    private static IDisposable TimeBlock(string name)
    {
        var sw = Stopwatch.StartNew();
        Console.WriteLine($"[t] ▶ {name}");
        return new ActionOnDispose(() =>
        {
            sw.Stop();
            Console.WriteLine($"[t] ✓ {name} = {FmtMs(sw.ElapsedMilliseconds)}");
        });
    }

    private static CancellationTokenSource StartWatchdog(TimeSpan limit, string label)
    {
        var cts = new CancellationTokenSource();
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(limit, cts.Token);
                Console.WriteLine($"[watchdog] ⏱ passou de {limit}. label={label}");
                Console.WriteLine(new StackTrace(true).ToString());
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Console.WriteLine("[watchdog-erro] " + ex.Message);
            }
        });
        return cts;
    }

    private static List<string>? TryParsePmus(string? pmusJson)
    {
        if (string.IsNullOrWhiteSpace(pmusJson))
            return null;

        try
        {
            var arr = JsonSerializer.Deserialize<List<string>>(pmusJson);
            if (arr == null || arr.Count == 0)
                return null;

            return arr
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        catch
        {
            return null;
        }
    }

    private sealed class ActionOnDispose : IDisposable
    {
        private readonly Action _action;

        public ActionOnDispose(Action action)
        {
            _action = action;
        }

        public void Dispose()
        {
            _action();
        }
    }
}
