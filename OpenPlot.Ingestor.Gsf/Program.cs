using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Npgsql;
using OpenPlot.Ingestor.Gsf.Repository;

namespace OpenPlot.Ingestor.Gsf
{
    internal static class Program
    {
        // ----------------- CONFIG -----------------
        private static string PgConnString;
        private static int PollIntervalSeconds;
        private static int ChunkMinutes;
        private static int MaxParallelChunks;
        private static int MaxParallelJobs;
        private static int GlobalMaxParallelChunks;
        private static SemaphoreSlim GlobalChunkLimiter;

        private sealed class SearchRunJob
        {
            public Guid Id { get; init; }
            public string Source { get; init; }
            public string TerminalId { get; init; }
            public string SignalsJson { get; init; }
            public string PmusJson { get; init; }
            public DateTime From { get; init; }
            public DateTime To { get; init; }
            public int SelectRate { get; init; }
        }

        // ----------------- TIMING HELPERS -----------------
        private static string FmtMs(long ms)
        {
            if (ms < 1000) return ms + "ms";
            var ts = TimeSpan.FromMilliseconds(ms);
            if (ts.TotalMinutes >= 1) return $"{(int)ts.TotalMinutes}m{ts.Seconds:D2}s";
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

        private sealed class ActionOnDispose : IDisposable
        {
            private readonly Action _a;
            public ActionOnDispose(Action a) => _a = a;
            public void Dispose() => _a();
        }

        // Watchdog: se passar do limite, imprime stacktrace (bom p/ travas/espera)
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
                catch (OperationCanceledException) { /* ok */ }
                catch (Exception ex) { Console.WriteLine("[watchdog-erro] " + ex.Message); }
            });
            return cts;
        }

        // ----------------- STATUS HELPERS -----------------
        private static void MarkBadConnection(NpgsqlConnection conn, Guid id, string details = null)
        {
            var msg = string.IsNullOrWhiteSpace(details) ? "bad_connection" : ("bad_connection: " + details);

            try
            {
                using (var tx = conn.BeginTransaction())
                {
                    DbOps.UpdateStatus(conn, tx, id, "bad_connection", 0, msg);
                    tx.Commit();
                }
            }
            catch
            {
                // noop
            }
        }

        // ----------------- PMUS_OK PERSISTENCE -----------------
        // Salva a lista de PMUs que efetivamente retornaram dados (inclui caso [skip] já existente, pois hasData=1).
        private static void SavePmusOk(NpgsqlConnection conn, NpgsqlTransaction tx, Guid jobId, List<string> pmusOk)
        {
            // Normaliza (evita nulos, remove espaços, distinct)
            var norm = (pmusOk ?? new List<string>())
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();

            // Decide: aqui salvamos sempre um array JSON (mesmo vazio => "[]")
            var json = JsonSerializer.Serialize(norm);

            using (var cmd = new NpgsqlCommand(@"
                UPDATE openplot.search_runs
                   SET pmus_ok = @pmus_ok::jsonb
                 WHERE id = @id;", conn, tx))
            {
                cmd.Parameters.AddWithValue("@id", jobId);
                cmd.Parameters.AddWithValue("@pmus_ok", json);
                cmd.ExecuteNonQuery();
            }
        }

        // ----------------- PROGRESS HELPERS -----------------
        // Progresso: calculado por "unidade de trabalho" = (PMU x chunk de tempo).
        // - total = (qtde PMUs) * (qtde intervals)
        // - done  = incrementa 1 ao finalizar cada chunk (skip/sem dados/ok/erro-chunk contam como concluído)
        // - throttling: atualiza no DB no máx a cada X ms OU quando variar >= 1%
        private sealed class ProgressReporter
        {
            private readonly string _connString;
            private readonly Guid _jobId;
            private readonly int _total;
            private readonly object _sync = new object();

            private readonly int _minStepPercent;
            private readonly TimeSpan _minInterval;

            private long _done;
            private int _lastPct;
            private long _lastTick;

            public ProgressReporter(string connString, Guid jobId, int total, int minStepPercent = 1, int minIntervalMs = 800)
            {
                _connString = connString;
                _jobId = jobId;
                _total = Math.Max(1, total);
                _minStepPercent = Math.Max(1, minStepPercent);
                _minInterval = TimeSpan.FromMilliseconds(Math.Max(200, minIntervalMs));
                _lastTick = Stopwatch.GetTimestamp();
                _lastPct = 0;
            }

            public int Total => _total;

            public void Tick(string msg = null)
            {
                var done = Interlocked.Increment(ref _done);
                int pct;

                lock (_sync)
                {
                    pct = (int)Math.Floor(100.0 * done / _total);

                    if (pct > 99) pct = 99;

                    var now = Stopwatch.GetTimestamp();
                    var elapsed = TimeSpan.FromSeconds((now - _lastTick) / (double)Stopwatch.Frequency);

                    if ((pct - _lastPct) < _minStepPercent && elapsed < _minInterval)
                        return;

                    _lastPct = pct;
                    _lastTick = now;
                }

                try
                {
                    using (var c = new NpgsqlConnection(_connString))
                    {
                        c.Open();
                        using (var tx = c.BeginTransaction())
                        {
                            DbOps.UpdateStatus(c, tx, _jobId, "running", pct, msg ?? $"Processando ({done}/{_total})");
                            tx.Commit();
                        }
                    }
                }
                catch
                {
                }
            }
        }

        private static int CountIntervals(DateTime fromUtc, DateTime toUtc)
        {
            var totalSpan = toUtc - fromUtc;
            if (totalSpan <= TimeSpan.Zero) return 1;

            var chunkSize = TimeSpan.FromMinutes(Math.Max(1, ChunkMinutes));
            if (chunkSize > totalSpan) chunkSize = totalSpan;

            int n = 0;
            for (var cs = fromUtc; cs < toUtc; cs = cs.Add(chunkSize))
                n++;

            return Math.Max(1, n);
        }

        private static void Main()
        {
            try
            {
                LoadConfig();

                using (var conn = new NpgsqlConnection(PgConnString))
                {
                    conn.Open();
                    DbOps.EnsureSchema(conn);
                }

                GlobalChunkLimiter = new SemaphoreSlim(GlobalMaxParallelChunks, GlobalMaxParallelChunks);

                Console.WriteLine("[ingestor] iniciado. Ctrl+C para sair.");
                Console.WriteLine("[ingestor] DB:  " + PgConnString);
                Console.WriteLine("[ingestor] workers=" + MaxParallelJobs + ", chunks/job=" + MaxParallelChunks + ", chunks globais=" + GlobalMaxParallelChunks);

                var workers = Enumerable.Range(1, MaxParallelJobs)
                    .Select(workerId => Task.Run(() => WorkerLoop(workerId)))
                    .ToArray();

                Task.WaitAll(workers);
            }
            catch (Exception exTop)
            {
                Console.WriteLine("[fatal] " + exTop.Message);
            }
        }

        private static void WorkerLoop(int workerId)
        {
            while (true)
            {
                try
                {
                    var job = TryPickQueuedJob();

                    if (job == null)
                    {
                        Thread.Sleep(PollIntervalSeconds * 1000);
                        continue;
                    }

                    ProcessJob(job, workerId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[worker " + workerId + "] " + ex.Message);
                    Thread.Sleep(PollIntervalSeconds * 1000);
                }
            }
        }

        private static SearchRunJob TryPickQueuedJob()
        {
            using (var conn = new NpgsqlConnection(PgConnString))
            {
                conn.Open();

                using (var tx = conn.BeginTransaction())
                {
                    const string pickSql = @"
                        SELECT id, source, terminal_id, signals::text, from_ts, to_ts, select_rate, pmus::text
                          FROM openplot.search_runs
                         WHERE status = 'queued'
                         ORDER BY created_at
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1;";

                    using (var cmd = new NpgsqlCommand(pickSql, conn, tx))
                    using (var rdr = cmd.ExecuteReader())
                    {
                        if (!rdr.Read())
                        {
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
                        DbOps.UpdateStatus(conn, tx, job.Id, "running", 1, "Iniciando");
                        tx.Commit();
                        return job;
                    }
                }
            }
        }

        private static void ProcessJob(SearchRunJob job, int workerId)
        {
            using (var conn = new NpgsqlConnection(PgConnString))
            {
                conn.Open();

                try
                {
                    using (TimeBlock($"JOB {job.Id} (worker={workerId}, source={job.Source}) from={job.From:O} to={job.To:O}"))
                    using (var wd = StartWatchdog(TimeSpan.FromMinutes(2), $"JOB {job.Id}"))
                    {
                        var fromUtc = job.From.Kind == DateTimeKind.Utc ? job.From : job.From.ToUniversalTime();
                        var toUtc = job.To.Kind == DateTimeKind.Utc ? job.To : job.To.ToUniversalTime();

                        var sysCfg = DbSystemDataFactory.BuildByPdcName(
                            PgConnString,
                            job.Source,
                            TimeSpan.FromMinutes(10)
                        );

                        var pmuList = TryParsePmus(job.PmusJson);
                        var nPmus = (pmuList != null && pmuList.Count > 0) ? pmuList.Count : 1;
                        var nIntervals = CountIntervals(fromUtc, toUtc);
                        var progress = new ProgressReporter(PgConnString, job.Id, nPmus * nIntervals);

                        List<string> pmusComDados = null;

                        if (pmuList != null && pmuList.Count > 0)
                        {
                            pmusComDados = new List<string>();

                            foreach (var pmuIdName in pmuList)
                            {
                                var term = TerminalResolver.Resolve(sysCfg, pmuIdName);
                                var channels = LoadChannelsFromDb(conn, job.Source, pmuIdName);

                                if (channels == null || channels.Count == 0)
                                    throw new Exception("Nenhum canal encontrado no DB para a PMU '" + pmuIdName + "'.");

                                var teveDados = FetchAndInsert(
                                    conn,
                                    job.Id,
                                    job.Source ?? sysCfg.Name,
                                    sysCfg,
                                    term,
                                    channels,
                                    fromUtc,
                                    toUtc,
                                    job.SelectRate,
                                    progress
                                );

                                if (teveDados)
                                    pmusComDados.Add(pmuIdName);
                            }
                        }

                        using (var tx2 = conn.BeginTransaction())
                        {
                            SavePmusOk(conn, tx2, job.Id, pmusComDados);

                            if (pmusComDados == null || pmusComDados.Count == 0)
                            {
                                DbOps.UpdateStatus(
                                    conn,
                                    tx2,
                                    job.Id,
                                    "no_data",
                                    100,
                                    "Consulta executada com sucesso, porém sem dados no intervalo solicitado"
                                );
                            }
                            else
                            {
                                DbOps.UpdateStatus(
                                    conn,
                                    tx2,
                                    job.Id,
                                    "done",
                                    100,
                                    "Concluído"
                                );
                            }

                            tx2.Commit();
                        }

                        wd.Cancel();
                    }
                }
                catch (InvalidConnectionException ex)
                {
                    Console.WriteLine("[bad_connection] job " + job.Id + ": " + ex.Message);
                    MarkBadConnection(conn, job.Id, ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[erro] job " + job.Id + ": " + ex.Message);
                    try
                    {
                        using (var tx2 = conn.BeginTransaction())
                        {
                            DbOps.UpdateStatus(conn, tx2, job.Id, "failed", 0, ex.Message);
                            tx2.Commit();
                        }
                    }
                    catch
                    {
                    }
                }
            }
        }

        // ----------------- PIPELINE -----------------
        private static bool FetchAndInsert(
            NpgsqlConnection conn,
            Guid jobId,
            string jobSource,
            SystemData systemCfg,
            Terminal term,
            List<Channel> channels,
            DateTime fromUtc,
            DateTime toUtc,
            int selectRate,
            ProgressReporter progress)
        {
            int hasData = 0;

            var ctx = GetPdcContext(conn, jobSource, term.Id);
            int pdcPmuId = ctx.pdcPmuId;

            var signalMap = LoadSignalMap(conn, pdcPmuId, channels);
            if (signalMap.Count == 0)
                throw new Exception("Nenhum signal mapeado para os Channel.Id informados (verifique o catálogo).");

            var allSignalIds = signalMap.Values.Distinct().ToArray();

            var totalSpan = toUtc - fromUtc;
            var chunkSize = TimeSpan.FromMinutes(Math.Max(1, ChunkMinutes));
            if (chunkSize > totalSpan) chunkSize = totalSpan;

            var intervals = new List<(DateTime cs, DateTime ce)>();
            for (var cs = fromUtc; cs < toUtc; cs = cs.Add(chunkSize))
            {
                var ce = cs.Add(chunkSize);
                if (ce > toUtc) ce = toUtc;
                intervals.Add((cs, ce));
            }

            using var cts = new CancellationTokenSource();
            InvalidConnectionException badConn = null;

            var po = new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(1, Math.Min(MaxParallelChunks, GlobalMaxParallelChunks)),
                CancellationToken = cts.Token
            };

            try
            {
                Parallel.ForEach(intervals, po, (interval, state) =>
                {
                    if (po.CancellationToken.IsCancellationRequested)
                        return;

                    var cs = interval.cs;
                    var ce = interval.ce;
                    var slotAcquired = false;

                    try
                    {
                        GlobalChunkLimiter?.Wait(po.CancellationToken);
                        slotAcquired = true;

                        if (po.CancellationToken.IsCancellationRequested)
                            return;

                        if (ChunkAlreadyPresentDb(PgConnString, pdcPmuId, allSignalIds, cs, ce))
                        {
                            Console.WriteLine("[skip] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " (já existente)");
                            Interlocked.Exchange(ref hasData, 1);
                            progress?.Tick($"Processando: {term.Id}");
                            return;
                        }

                        var repo = RepositoryFactory.Create(systemCfg);

                        string terminalCode;
                        if (systemCfg.Type == DatabaseType.Medfasee)
                            terminalCode = term.IdNumber.ToString();
                        else
                            terminalCode = term.Id;

                        var dict = repo.QueryTerminalSeries(
                            terminalCode,
                            cs, ce,
                            channels,
                            selectRate,
                            term.EquipmentRate,
                            false);

                        if (dict == null || dict.Count == 0)
                        {
                            Console.WriteLine("[info] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " sem dados");
                            progress?.Tick($"Processando: {term.Id}");
                            return;
                        }

                        using (var connCopy = new NpgsqlConnection(PgConnString))
                        {
                            connCopy.Open();
                            using (var txCopy = connCopy.BeginTransaction())
                            {
                                using (var cmd = new NpgsqlCommand(@"
                                CREATE TEMP TABLE IF NOT EXISTS measurements_stage_tmp (
                                    ts          timestamptz       NOT NULL,
                                    pdc_pmu_id  integer           NOT NULL,
                                    signal_id   integer           NOT NULL,
                                    value       double precision  NOT NULL
                                ) ON COMMIT DROP;
                                TRUNCATE measurements_stage_tmp;", connCopy, txCopy))
                                {
                                    cmd.ExecuteNonQuery();
                                }

                                using (var imp = connCopy.BeginBinaryImport(@"
                                COPY measurements_stage_tmp
                                (ts, pdc_pmu_id, signal_id, value)
                                FROM STDIN (FORMAT BINARY)"))
                                {
                                    foreach (var kv in dict)
                                    {
                                        var ch = kv.Key;
                                        var series = kv.Value;
                                        if (series == null || series.Count == 0) continue;

                                        var key = (ch.Id, ch.Quantity, ch.Phase, ch.Value);

                                        if (!signalMap.TryGetValue(key, out var sigId))
                                            continue;

                                        var ts = series.GetTimestamps();
                                        var rd = series.GetReadings();

                                        for (int i = 0; i < series.Count; i++)
                                        {
                                            var dt = FromOADateUtc(ts[i]);
                                            var val = rd[i];
                                            if (double.IsNaN(val) || double.IsInfinity(val)) continue;
                                            if (dt.Year < 1970 || dt.Year > 2100) continue;

                                            imp.StartRow();
                                            imp.Write(dt, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                                            imp.Write(pdcPmuId, NpgsqlTypes.NpgsqlDbType.Integer);
                                            imp.Write(sigId, NpgsqlTypes.NpgsqlDbType.Integer);
                                            imp.Write(val, NpgsqlTypes.NpgsqlDbType.Double);
                                        }
                                    }

                                    imp.Complete();
                                }

                                using (var upsert = new NpgsqlCommand(@"
                                INSERT INTO openplot.measurements (ts, pdc_pmu_id, signal_id, value)
                                SELECT ts, pdc_pmu_id, signal_id, value
                                  FROM measurements_stage_tmp
                                ON CONFLICT (pdc_pmu_id, signal_id, ts) DO NOTHING;", connCopy, txCopy))
                                {
                                    upsert.ExecuteNonQuery();
                                }

                                txCopy.Commit();
                                Interlocked.Exchange(ref hasData, 1);
                                Console.WriteLine("[ok] " + term.Id + " " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " inserido");
                            }
                        }

                        progress?.Tick($"Processando: {term.Id}");
                    }
                    catch (InvalidConnectionException ex)
                    {
                        if (Interlocked.CompareExchange(ref badConn, ex, null) == null)
                        {
                            cts.Cancel();
                            state.Stop();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[erro-chunk] " + term.Id + " " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + ": " + ex.Message);
                        progress?.Tick($"Processando: {term.Id}");
                    }
                    finally
                    {
                        if (slotAcquired)
                            GlobalChunkLimiter?.Release();
                    }
                });
            }
            catch (OperationCanceledException)
            {
            }

            if (badConn != null)
                throw badConn;

            return hasData != 0;
        }

        // ----------------- HELPERS (BD / MAPAS) -----------------
        static List<OpenPlot.Ingestor.Gsf.Channel> LoadChannelsFromDb(NpgsqlConnection conn, string source, string pmuIdName)
        {
            const string sql = @"
        SELECT
            s.historian_point,
            s.name,
            s.quantity,
            s.phase,
            s.component
        FROM openplot.signal s
        JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
        JOIN openplot.pmu     p   ON p.pmu_id       = ppm.pmu_id
        JOIN openplot.pdc     d   ON d.pdc_id       = ppm.pdc_id
        WHERE d.name = @source
          AND (p.id_name = @pmu OR ppm.pdc_local_id = @pmu)
        ORDER BY s.quantity, s.component, s.phase, s.signal_id;";

            var list = new List<OpenPlot.Ingestor.Gsf.Channel>();

            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@source", source);
                cmd.Parameters.AddWithValue("@pmu", pmuIdName);

                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        var pointId = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
                        var chName = rdr.IsDBNull(1) ? "" : rdr.GetString(1);
                        var qtyStr = rdr.IsDBNull(2) ? "" : rdr.GetString(2);
                        var phaseStr = rdr.IsDBNull(3) ? "" : rdr.GetString(3);
                        var compStr = rdr.IsDBNull(4) ? "" : rdr.GetString(4);

                        var qty = GetQuantityFromDb(qtyStr, compStr);
                        var vtype = GetValueTypeFromDb(qty, compStr);
                        var phase = GetPhaseFromDb(phaseStr);

                        list.Add(new OpenPlot.Ingestor.Gsf.Channel(pointId, chName, phase, vtype, qty));
                    }
                }
            }

            return list;
        }

        // ---------- Mapas: DB -> enums usados por Channel ----------
        static OpenPlot.Ingestor.Gsf.ChannelQuantity GetQuantityFromDb(string qty, string component)
        {
            if (string.Equals(qty, "Voltage", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelQuantity.VOLTAGE;
            if (string.Equals(qty, "Current", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelQuantity.CURRENT;

            if (string.Equals(qty, "Frequency", StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(component, "DFREQ", StringComparison.OrdinalIgnoreCase))
                    return OpenPlot.Ingestor.Gsf.ChannelQuantity.DFREQ;
                return OpenPlot.Ingestor.Gsf.ChannelQuantity.FREQUENCY;
            }

            // digital
            if (string.Equals(qty, "Digital", StringComparison.OrdinalIgnoreCase))
                return OpenPlot.Ingestor.Gsf.ChannelQuantity.DIGITAL;

            return OpenPlot.Ingestor.Gsf.ChannelQuantity.ANALOG;
        }

        static OpenPlot.Ingestor.Gsf.ChannelPhase GetPhaseFromDb(string ph)
        {
            if (string.Equals(ph, "A", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelPhase.PHASE_A;
            if (string.Equals(ph, "B", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelPhase.PHASE_B;
            if (string.Equals(ph, "C", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelPhase.PHASE_C;
            return OpenPlot.Ingestor.Gsf.ChannelPhase.NONE;
        }

        static OpenPlot.Ingestor.Gsf.ChannelValueType GetValueTypeFromDb(OpenPlot.Ingestor.Gsf.ChannelQuantity q, string component)
        {
            if (q == OpenPlot.Ingestor.Gsf.ChannelQuantity.VOLTAGE || q == OpenPlot.Ingestor.Gsf.ChannelQuantity.CURRENT)
            {
                if (string.Equals(component, "MAG", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelValueType.ABSOLUTE;
                if (string.Equals(component, "ANG", StringComparison.OrdinalIgnoreCase)) return OpenPlot.Ingestor.Gsf.ChannelValueType.ANGLE;
            }

            // digital
            if (q == OpenPlot.Ingestor.Gsf.ChannelQuantity.DIGITAL)
                return OpenPlot.Ingestor.Gsf.ChannelValueType.ABSOLUTE;

            return OpenPlot.Ingestor.Gsf.ChannelValueType.NONE;
        }

        static List<string> TryParsePmus(string pmusJson)
        {
            if (string.IsNullOrWhiteSpace(pmusJson))
                return null;

            try
            {
                var arr = JsonSerializer.Deserialize<List<string>>(pmusJson);
                if (arr == null || arr.Count == 0)
                    return null;

                var list = arr
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                return list;
            }
            catch
            {
                return null;
            }
        }

        private static (int pdcId, int pmuId, int pdcPmuId) GetPdcContext(NpgsqlConnection conn, string pdcName, string pmuIdName)
        {
            using (var cmd = new NpgsqlCommand(@"
                SELECT p.pdc_id, u.pmu_id, pp.pdc_pmu_id
                  FROM openplot.pdc        p
                  JOIN openplot.pdc_pmu    pp ON pp.pdc_id = p.pdc_id
                  JOIN openplot.pmu        u  ON u.pmu_id = pp.pmu_id
                 WHERE p.name = @pdc AND u.id_name = @pmu;", conn))
            {
                cmd.Parameters.AddWithValue("pdc", pdcName);
                cmd.Parameters.AddWithValue("pmu", pmuIdName);

                using (var r = cmd.ExecuteReader())
                {
                    if (!r.Read())
                        throw new Exception("Contexto pdc/pmu não encontrado (pdc='" + pdcName + "', pmu='" + pmuIdName + "').");
                    return (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2));
                }
            }
        }

        private static Dictionary<(int hist,
                           OpenPlot.Ingestor.Gsf.ChannelQuantity qty,
                           OpenPlot.Ingestor.Gsf.ChannelPhase phase,
                           OpenPlot.Ingestor.Gsf.ChannelValueType val), int>
        LoadSignalMap(
            NpgsqlConnection conn,
            int pdcPmuId,
            IEnumerable<Channel> channels)
        {
            var chList = channels.ToList();
            if (chList.Count == 0)
                return new Dictionary<(int hist,
                                       ChannelQuantity qty,
                                       ChannelPhase phase,
                                       ChannelValueType val), int>();

            var histIds = chList.Select(c => c.Id).Distinct().ToArray();
            if (histIds.Length == 0)
                return new Dictionary<(int hist,
                                       ChannelQuantity qty,
                                       ChannelPhase phase,
                                       ChannelValueType val), int>();

            using (var cmd = new NpgsqlCommand(@"
        SELECT
            historian_point,
            quantity::text,
            phase::text,
            component::text,
            signal_id
        FROM openplot.signal
        WHERE pdc_pmu_id = @pp
          AND historian_point = ANY(@hids);", conn))
            {
                cmd.Parameters.AddWithValue("pp", pdcPmuId);
                cmd.Parameters.AddWithValue("hids", histIds);

                using (var r = cmd.ExecuteReader())
                {
                    var map = new Dictionary<(int,
                                              OpenPlot.Ingestor.Gsf.ChannelQuantity,
                                              OpenPlot.Ingestor.Gsf.ChannelPhase,
                                              OpenPlot.Ingestor.Gsf.ChannelValueType), int>();

                    while (r.Read())
                    {
                        var hist = r.GetInt32(0);
                        var qtyStr = r.IsDBNull(1) ? "" : r.GetString(1);
                        var phaseStr = r.IsDBNull(2) ? "" : r.GetString(2);
                        var compStr = r.IsDBNull(3) ? "" : r.GetString(3);
                        var sigId = r.GetInt32(4);

                        var qty = GetQuantityFromDb(qtyStr, compStr);
                        var phase = GetPhaseFromDb(phaseStr);
                        var vtype = GetValueTypeFromDb(qty, compStr);

                        var key = (hist, qty, phase, vtype);

                        if (!map.ContainsKey(key))
                            map[key] = sigId;
                    }

                    return map;
                }
            }
        }

        private static bool ChunkAlreadyPresentDb(string connString, int pdcPmuId, int[] signalIds, DateTime from, DateTime to)
        {
            if (signalIds == null || signalIds.Length == 0) return false;

            using (var c = new NpgsqlConnection(connString))
            {
                c.Open();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT COUNT(DISTINCT signal_id)
                      FROM openplot.measurements
                     WHERE pdc_pmu_id = @pp
                       AND signal_id   = ANY(@sids)
                       AND ts >= @from AND ts < @to;", c))
                {
                    cmd.Parameters.AddWithValue("pp", pdcPmuId);
                    cmd.Parameters.AddWithValue("sids", signalIds);
                    cmd.Parameters.AddWithValue("from", from);
                    cmd.Parameters.AddWithValue("to", to);

                    var countObj = cmd.ExecuteScalar();
                    var count = (countObj == null || countObj is DBNull) ? 0L : Convert.ToInt64(countObj);

                    return count >= signalIds.Length;
                }
            }
        }

        private static List<string> ParseSignals(string jsonArray)
        {
            if (string.IsNullOrWhiteSpace(jsonArray)) return new List<string>();
            try
            {
                var arr = JArray.Parse(jsonArray);
                return arr.Select(j => j.ToString()).ToList();
            }
            catch
            {
                return new List<string> { jsonArray };
            }
        }

        static readonly TimeZoneInfo TzBr = TimeZoneInfo.FindSystemTimeZoneById("E. South America Standard Time");

        static DateTime FromOADateUtc(double oa)
        {
            // O valor de OADate já representa UTC.
            var dt = DateTime.FromOADate(oa);          // Kind = Unspecified
            return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        }

        private static void LoadConfig()
        {
            PgConnString = ConfigurationManager.AppSettings["Db"];
            PollIntervalSeconds = ReadInt("PollIntervalSeconds", 2);
            ChunkMinutes = ReadInt("ChunkMinutes", 5);

            var cpuCount = Math.Max(1, Environment.ProcessorCount);
            GlobalMaxParallelChunks = Math.Max(1, Math.Min(ReadInt("GlobalMaxParallelChunks", cpuCount), cpuCount));
            MaxParallelChunks = Math.Max(1, Math.Min(ReadInt("MaxParallelChunks", 4), GlobalMaxParallelChunks));
            MaxParallelJobs = Math.Max(1, Math.Min(ReadInt("MaxParallelJobs", Math.Min(2, GlobalMaxParallelChunks)), GlobalMaxParallelChunks));

            if (string.IsNullOrWhiteSpace(PgConnString))
                throw new Exception("App.config: defina AppSettings key=Db.");
        }

        private static int ReadInt(string key, int def)
        {
            var v = ConfigurationManager.AppSettings[key];
            int n;
            return int.TryParse(v, out n) ? n : def;
        }
    }
}