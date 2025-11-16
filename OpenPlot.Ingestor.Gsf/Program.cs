using Newtonsoft.Json.Linq;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Ingestor.Gsf
{
    internal static class Program
    {
        // ----------------- CONFIG -----------------
        private static string PgConnString;
        private static int PollIntervalSeconds;
        private static int ChunkMinutes;
        private static int MaxParallelChunks;

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
                            // 👉 Acrescenta pmus::text no SELECT (fica a última coluna)
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
                                    pmusJson = rdr.IsDBNull(7) ? null : rdr.GetString(7); // 👈 NOVO
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
                                    // usa UTC no pipeline
                                    var fromUtc = from.Kind == DateTimeKind.Utc ? from : from.ToUniversalTime();
                                    var toUtc = to.Kind == DateTimeKind.Utc ? to : to.ToUniversalTime();

                                    // ================================
                                    // 1) Monta SystemData a partir do BD
                                    //    (usa cache interno por pdc_id)
                                    // ================================
                                    // 1) Monta SystemData a partir do BD (usa cache interno por pdc_id)
                                    var sysCfg = DbSystemDataFactory.BuildByPdcName(
                                        PgConnString,
                                        source,
                                        TimeSpan.FromMinutes(10)
                                    );



                                    // ========= NOVO CAMINHO: pmus em search_runs =========
                                    var pmuList = TryParsePmus(pmusJson);
                                    if (pmuList != null && pmuList.Count > 0)
                                    {
                                        foreach (var pmuIdName in pmuList)
                                        {
                                            // Terminal vem do SystemData montado pelo DB
                                            var term = TerminalResolver.Resolve(sysCfg, pmuIdName);
                                            var channels = LoadChannelsFromDb(conn, source, pmuIdName); // canais vindos do DB (Id = historian_point)

                                            if (channels == null || channels.Count == 0)
                                                throw new Exception("Nenhum canal encontrado no DB para a PMU '" + pmuIdName + "'.");

                                            FetchAndInsert(
                                                conn,
                                                id,
                                                source ?? sysCfg.Name,
                                                sysCfg,
                                                term,
                                                channels,
                                                fromUtc,
                                                toUtc,
                                                selectRate
                                            );
                                        }
                                    }
                                    else
                                    {
                                        /*
                                        // ========= MODO LEGADO (inalterado) =========
                                        var term = TerminalResolver.Resolve(sysCfg, terminalId);
                                        var signals = ParseSignals(signalsJson);
                                        var channels = TerminalResolver.MapChannels(term, signals);
                                        if (channels == null || channels.Count == 0)
                                            throw new Exception("Nenhum canal mapeado para os sinais requisitados.");

                                        FetchAndInsert(
                                            conn,
                                            id,
                                            source ?? sysCfg.Name,
                                            sysCfg,
                                            term,
                                            channels,
                                            fromUtc,
                                            toUtc,
                                            selectRate
                                        );
                                        */
                                    }

                                    using (var tx2 = conn.BeginTransaction())
                                    {
                                        DbOps.UpdateStatus(conn, tx2, id, "done", 100, "Concluído");
                                        tx2.Commit();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("[erro] job " + id + ": " + ex.Message);
                                    try
                                    {
                                        using (var tx2 = conn.BeginTransaction())
                                        {
                                            DbOps.UpdateStatus(conn, tx2, id, "done", 100, "Concluído");
                                            tx2.Commit();
                                        }
                                    }
                                    catch
                                    {
                                        // noop
                                    }

                                    using (var tx2 = conn.BeginTransaction())
                                    {
                                        DbOps.UpdateStatus(conn, tx2, id, "failed", 0, ex.Message);
                                        tx2.Commit();
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
        private static void FetchAndInsert(
            NpgsqlConnection conn,
            Guid jobId,
            string jobSource,            // *** nome do PDC vindo do job (preferível)
            SystemData systemCfg,
            Terminal term,
            List<Channel> channels,
            DateTime fromUtc,
            DateTime toUtc,
            int selectRate)
        {
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

            var po = new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, MaxParallelChunks) };

            Parallel.ForEach(intervals, po, interval =>
            {
                var cs = interval.cs;
                var ce = interval.ce;

                try
                {
                    // 4) dedupe (meia-aberta no SQL também)
                    if (ChunkAlreadyPresentDb(PgConnString, pdcPmuId, allSignalIds, cs, ce))
                    {
                        Console.WriteLine("[skip] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " (já existente)");
                        return;
                    }

                    // 5) consulta historian
                    var repo = RepositoryFactory.Create(systemCfg);

                    // Escolhe o “código da PMU” conforme o tipo de banco
                    // - MedFasee  -> usa idNumber (o <idNumber> do XML)
                    // - Historian -> usa id_name (term.Id)
                    string terminalCode;

                    if (systemCfg.Type == DatabaseType.Medfasee)
                    {
                        terminalCode = term.IdNumber.ToString();
                    }
                    else
                    {
                        terminalCode = term.Id; // openpdc / openhistorian2 usam id_name
                    }

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
                                    var ch = kv.Key;      // Channel
                                    var series = kv.Value;
                                    if (series == null || series.Count == 0) continue;

                                    var key = (ch.Id, ch.Quantity, ch.Phase, ch.Value);

                                    if (!signalMap.TryGetValue(key, out var sigId))
                                        continue; // não mapeado no catálogo (signal)

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
                            Console.WriteLine("[ok] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " inserido");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[erro-chunk] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + ": " + ex.Message);
                }
            });
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
                        var qtyStr = r.IsDBNull(1) ? "" : r.GetString(1);  // "Voltage", "Frequency"
                        var phaseStr = r.IsDBNull(2) ? "" : r.GetString(2);  // "A","B","C","None"
                        var compStr = r.IsDBNull(3) ? "" : r.GetString(3);  // "MAG","ANG","FREQ","DFREQ"
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

        private static DateTime FromOADateUtc(double oa)
        {
            var dtLocal = DateTime.FromOADate(oa);
            return DateTime.SpecifyKind(dtLocal, DateTimeKind.Local).ToUniversalTime();
        }

        private static void LoadConfig()
        {
            PgConnString = ConfigurationManager.AppSettings["Db"];
            PollIntervalSeconds = ReadInt("PollIntervalSeconds", 2);
            ChunkMinutes = ReadInt("ChunkMinutes", 10);
            MaxParallelChunks = ReadInt("MaxParallelChunks", 2);

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
