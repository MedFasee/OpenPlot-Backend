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
                var pct = (int)Math.Floor(100.0 * done / _total);

                // Reserva 100% para o encerramento (done/no_data). Aqui fica no máximo 99%.
                if (pct > 99) pct = 99;

                var now = Stopwatch.GetTimestamp();
                var elapsed = TimeSpan.FromSeconds((now - _lastTick) / (double)Stopwatch.Frequency);

                // throttle por passo (%) ou tempo
                if ((pct - _lastPct) < _minStepPercent && elapsed < _minInterval)
                    return;

                _lastPct = pct;
                _lastTick = now;

                try
                {
                    using (var c = new NpgsqlConnection(_connString))
                    {
                        c.Open();
                        using (var tx = c.BeginTransaction())
                        {
                            // Mantém status 'running' e atualiza progress.
                            // msg curta para O&M / UI (opcional)
                            DbOps.UpdateStatus(c, tx, _jobId, "running", pct, msg ?? $"Processando ({done}/{_total})");
                            tx.Commit();
                        }
                    }
                }
                catch
                {
                    // noop (não quebra o job por falha de update de progresso)
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

                Console.WriteLine("[ingestor] iniciado. Ctrl+C para sair.");
                Console.WriteLine("[ingestor] DB:  " + PgConnString);

                while (true)
                {
                    bool found;

                    using (var conn = new NpgsqlConnection(PgConnString))
                    {
                        conn.Open();
                        DbOps.EnsureSchema(conn);

                        using (var tx = conn.BeginTransaction())
                        {
                            // Acrescenta pmus::text no SELECT (fica a última coluna)
                            const string pickSql = @"
                        SELECT id, source, terminal_id, signals::text, from_ts, to_ts, select_rate, pmus::text
                          FROM openplot.search_runs
                         WHERE status = 'queued'
                         ORDER BY created_at
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1;";

                            Guid id = Guid.Empty;
                            string source = null, terminalId = null, signalsJson = null, pmusJson = null;
                            DateTime from = default(DateTime), to = default(DateTime);
                            int selectRate = 0;

                            using (var cmd = new NpgsqlCommand(pickSql, conn, tx))
                            using (var rdr = cmd.ExecuteReader())
                            {
                                if (!rdr.Read())
                                {
                                    found = false;
                                }
                                else
                                {
                                    found = true;
                                    id = rdr.GetGuid(0);
                                    source = rdr.GetString(1);
                                    terminalId = rdr.IsDBNull(2) ? null : rdr.GetString(2);
                                    signalsJson = rdr.GetString(3);
                                    from = rdr.GetDateTime(4);
                                    to = rdr.GetDateTime(5);
                                    selectRate = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6);
                                    pmusJson = rdr.IsDBNull(7) ? null : rdr.GetString(7);
                                }
                            }

                            if (!found)
                            {
                                tx.Commit();
                            }
                            else
                            {
                                // Marca running e libera o lock o quanto antes
                                DbOps.UpdateStatus(conn, tx, id, "running", 1, "Iniciando");
                                tx.Commit();

                                try
                                {
                                    using (TimeBlock($"JOB {id} (source={source}) from={from:O} to={to:O}"))
                                    using (var wd = StartWatchdog(TimeSpan.FromMinutes(2), $"JOB {id}"))
                                    {
                                        var fromUtc = from.Kind == DateTimeKind.Utc ? from : from.ToUniversalTime();
                                        var toUtc = to.Kind == DateTimeKind.Utc ? to : to.ToUniversalTime();

                                        // 1) Monta SystemData a partir do BD (usa cache interno por pdc_id)
                                        var sysCfg = DbSystemDataFactory.BuildByPdcName(
                                            PgConnString,
                                            source,
                                            TimeSpan.FromMinutes(10)
                                        );

                                        // Lista de PMUs pedidas nesse job
                                        var pmuList = TryParsePmus(pmusJson);

                                        // Progresso do job (PMU x chunk)
                                        // - Se pmuList não vier, assume 1 (caminho legado desabilitado no momento)
                                        var nPmus = (pmuList != null && pmuList.Count > 0) ? pmuList.Count : 1;
                                        var nIntervals = CountIntervals(fromUtc, toUtc);
                                        var progress = new ProgressReporter(PgConnString, id, nPmus * nIntervals);

                                        // Vamos acumular só as PMUs que efetivamente tiveram dados
                                        List<string> pmusComDados = null;

                                        if (pmuList != null && pmuList.Count > 0)
                                        {
                                            pmusComDados = new List<string>();

                                            foreach (var pmuIdName in pmuList)
                                            {
                                                // Terminal vem do SystemData montado pelo DB
                                                var term = TerminalResolver.Resolve(sysCfg, pmuIdName);

                                                // Canais vindos do DB (Id = historian_point)
                                                var channels = LoadChannelsFromDb(conn, source, pmuIdName);

                                                if (channels == null || channels.Count == 0)
                                                    throw new Exception("Nenhum canal encontrado no DB para a PMU '" + pmuIdName + "'.");

                                                // FetchAndInsert pode lançar InvalidConnectionException (bad_connection)
                                                var teveDados = FetchAndInsert(
                                                    conn,
                                                    id,
                                                    source ?? sysCfg.Name,
                                                    sysCfg,
                                                    term,
                                                    channels,
                                                    fromUtc,
                                                    toUtc,
                                                    selectRate,
                                                    progress
                                                );

                                                if (teveDados)
                                                    pmusComDados.Add(pmuIdName);
                                            }
                                        }
                                        else
                                        {
                                            // Caminho legado (sem pmus em search_runs)
                                            /*
                                            var term = TerminalResolver.Resolve(sysCfg, terminalId);
                                            var signals = ParseSignals(signalsJson);
                                            var channels = TerminalResolver.MapChannels(term, signals);
                                            if (channels == null || channels.Count == 0)
                                                throw new Exception("Nenhum canal mapeado para os sinais requisitados.");

                                            var teveDados = FetchAndInsert(
                                                conn,
                                                id,
                                                source ?? sysCfg.Name,
                                                sysCfg,
                                                term,
                                                channels,
                                                fromUtc,
                                                toUtc,
                                                selectRate,
                                                progress
                                            );

                                            if (teveDados)
                                                pmusComDados = new List<string> { term.Id };
                                            */
                                        }

                                        // Ao final do job, marcamos DONE e, se for o caso, salvamos pmus_ok
                                        using (var tx2 = conn.BeginTransaction())
                                        {
                                            // Adição: status terminal informativo quando a consulta executa com sucesso,
                                            // porém não retorna dados em todo o intervalo solicitado.
                                            if (pmusComDados == null || pmusComDados.Count == 0)
                                            {
                                                DbOps.UpdateStatus(
                                                    conn,
                                                    tx2,
                                                    id,
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
                                                    id,
                                                    "done",
                                                    100,
                                                    "Concluído"
                                                );
                                            }

                                            // Importante: commit da transação final (status done/no_data)
                                            tx2.Commit();
                                        }

                                        wd.Cancel();
                                    }
                                }
                                catch (InvalidConnectionException ex)
                                {
                                    // >>> COMPORTAMENTO 1: bad_connection + progress=0 + encerra tentativa
                                    Console.WriteLine("[bad_connection] job " + id + ": " + ex.Message);
                                    MarkBadConnection(conn, id, ex.Message);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("[erro] job " + id + ": " + ex.Message);
                                    try
                                    {
                                        using (var tx2 = conn.BeginTransaction())
                                        {
                                            DbOps.UpdateStatus(conn, tx2, id, "failed", 0, ex.Message);
                                            tx2.Commit();
                                        }
                                    }
                                    catch
                                    {
                                        // noop
                                    }
                                }
                            }
                        }
                    }

                    if (!found)
                        Thread.Sleep(PollIntervalSeconds * 1000);
                }
            }
            catch (Exception exTop)
            {
                Console.WriteLine("[fatal] " + exTop.Message);
            }
        }

        // ----------------- PIPELINE -----------------
        private static bool FetchAndInsert(
            NpgsqlConnection conn,
            Guid jobId,
            string jobSource,            // nome do PDC vindo do job
            SystemData systemCfg,
            Terminal term,
            List<Channel> channels,
            DateTime fromUtc,
            DateTime toUtc,
            int selectRate,
            ProgressReporter progress)   // <<< progresso do job
        {
            // Flag: 0 = não teve dado, 1 = teve dado (novo ou já existente)
            int hasData = 0;

            // 1) contexto no catálogo (pdc / pmu / pdc_pmu)
            var ctx = GetPdcContext(conn, jobSource, term.Id);  // term.Id == pmu.id_name
            int pdcPmuId = ctx.pdcPmuId;

            // 2) mapa historian_point -> signal_id dentro do pdc_pmu
            var signalMap = LoadSignalMap(conn, pdcPmuId, channels);
            if (signalMap.Count == 0)
                throw new Exception("Nenhum signal mapeado para os Channel.Id informados (verifique o catálogo).");

            var allSignalIds = signalMap.Values.Distinct().ToArray();

            // 3) fatiamento meia-aberta [cs, ce)
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

            // >>> Para abortar todos os chunks se houver bad_connection
            using var cts = new CancellationTokenSource();
            InvalidConnectionException badConn = null;

            var po = new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(1, MaxParallelChunks),
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

                    try
                    {
                        // 4) dedupe (meia-aberta no SQL também)
                        if (ChunkAlreadyPresentDb(PgConnString, pdcPmuId, allSignalIds, cs, ce))
                        {
                            Console.WriteLine("[skip] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " (já existente)");
                            Interlocked.Exchange(ref hasData, 1);

                            // Progresso: este chunk foi "concluído"
                            progress?.Tick($"Processando: {term.Id}");

                            return;
                        }

                        // 5) consulta historian
                        var repo = RepositoryFactory.Create(systemCfg);

                        // Escolhe o “código da PMU” conforme o tipo de banco
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

                            // Progresso: este chunk foi "concluído"
                            progress?.Tick($"Processando: {term.Id}");

                            return;
                        }

                        // 6) staging + upsert (1 conexão por thread)
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

                        // Progresso: este chunk foi "concluído"
                        progress?.Tick($"Processando: {term.Id}");
                    }
                    catch (InvalidConnectionException ex)
                    {
                        // >>> bad_connection: aborta todos os chunks e propaga para o Main marcar status
                        badConn = ex;
                        cts.Cancel();
                        state.Stop();
                    }
                    catch (OperationCanceledException)
                    {
                        // cancelado por bad_connection
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[erro-chunk] " + term.Id + " " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + ": " + ex.Message);

                        // Progresso: mesmo com erro de chunk, este chunk foi "concluído"
                        progress?.Tick($"Processando: {term.Id}");
                    }
                });
            }
            catch (OperationCanceledException)
            {
                // cancelado por bad_connection
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
            MaxParallelChunks = ReadInt("MaxParallelChunks", 10);

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
