using System;
using System.Diagnostics;
using System.Threading;
using Npgsql;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed class IngestorProgressReporter
{
    private readonly string _connString;
    private readonly Guid _jobId;
    private readonly int _total;
    private readonly object _sync = new();
    private readonly int _minStepPercent;
    private readonly TimeSpan _minInterval;
    private long _done;
    private int _lastPct;
    private long _lastTick;

    public IngestorProgressReporter(string connString, Guid jobId, int total, int minStepPercent = 1, int minIntervalMs = 800)
    {
        _connString = connString;
        _jobId = jobId;
        _total = Math.Max(1, total);
        _minStepPercent = Math.Max(1, minStepPercent);
        _minInterval = TimeSpan.FromMilliseconds(Math.Max(200, minIntervalMs));
        _lastTick = Stopwatch.GetTimestamp();
        _lastPct = 0;
    }

    public void Tick(string? msg = null)
    {
        var done = Interlocked.Increment(ref _done);
        int pct;

        lock (_sync)
        {
            pct = (int)Math.Floor(100.0 * done / _total);
            if (pct > 99)
                pct = 99;

            var now = Stopwatch.GetTimestamp();
            var elapsed = TimeSpan.FromSeconds((now - _lastTick) / (double)Stopwatch.Frequency);
            if ((pct - _lastPct) < _minStepPercent && elapsed < _minInterval)
                return;

            _lastPct = pct;
            _lastTick = now;
        }

        try
        {
            using var conn = new NpgsqlConnection(_connString);
            conn.Open();
            using var tx = conn.BeginTransaction();
            DbOps.UpdateStatus(conn, tx, _jobId, "running", pct, msg ?? $"Processando ({done}/{_total})");
            tx.Commit();
        }
        catch
        {
        }
    }
}
