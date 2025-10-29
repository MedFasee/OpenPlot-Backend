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


namespace Medi.Ingestor.Gsf
{
    internal static class Program
    {
        // ----------------- CONFIG -----------------
        private static string PgConnString;
        private static string XmlPath;
        private static int PollIntervalSeconds;
        private static int ChunkMinutes;
        private static int MaxParallelChunks;
        private static SystemData SystemCfg;

        private static void Main()
        {
            try
            {
                LoadConfig();
                SystemCfg = SystemData.ReadConfig(XmlPath);

                Console.WriteLine("[ingestor] iniciado. Ctrl+C para sair.");
                Console.WriteLine("[ingestor] XML: " + XmlPath);
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
                            DateTime from = default, to = default;
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

                                    // ========= NOVO CAMINHO: pmus em search_runs =========
                                    var pmuList = TryParsePmus(pmusJson);
                                    if (pmuList != null && pmuList.Count > 0)
                                    {
                                        foreach (var pmuIdName in pmuList)
                                        {
                                            var term = TerminalResolver.Resolve(SystemCfg, pmuIdName); // mantém o Terminal do XML (metadados)
                                            var channels = LoadChannelsFromDb(conn, source, pmuIdName); // canais vindos do DB (Id = historian_point)

                                            if (channels == null || channels.Count == 0)
                                                throw new Exception("Nenhum canal encontrado no DB para a PMU '" + pmuIdName + "'.");

                                            FetchAndInsert(conn, id, source ?? SystemCfg.Name, SystemCfg,
                                                           term, channels, fromUtc, toUtc, selectRate);

                                        }
                                    }
                                    else
                                    {
                                        // ========= MODO LEGADO (inalterado) =========
                                        var term = TerminalResolver.Resolve(SystemCfg, terminalId);
                                        var signals = ParseSignals(signalsJson);
                                        var channels = TerminalResolver.MapChannels(term, signals);
                                        if (channels == null || channels.Count == 0)
                                            throw new Exception("Nenhum canal mapeado para os sinais requisitados.");

                                        FetchAndInsert(conn, id, source ?? SystemCfg.Name, SystemCfg,
                                                       term, channels, fromUtc, toUtc, selectRate);
                                    }

                                    using (var tx2 = conn.BeginTransaction())
                                    {
                                        DbOps.UpdateStatus(conn, tx2, id, "done", 100, "Concluído");
                                        tx2.Commit();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[erro] job {id}: {ex.Message}");
                                    try {
                                        using (var tx2 = conn.BeginTransaction())
                                        {
                                            DbOps.UpdateStatus(conn, tx2, id, "done", 100, "Concluído");
                                            tx2.Commit();
                                        }
                                    }
                                    catch { /* noop */ }

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
                        Console.WriteLine($"[skip] {cs:yyyy-MM-dd HH:mm}-{ce:HH:mm} (já existente)");
                        return;
                    }

                    // 5) consulta historian
                    var repo = RepositoryFactory.Create(systemCfg);
                    var dict = repo.QueryTerminalSeries(
                        term.IdNumber.ToString(),   // *** esta implementação precisa do número
                        cs, ce,
                        channels,
                        selectRate,
                        term.EquipmentRate,
                        false);

                    if (dict == null || dict.Count == 0)
                    {
                        Console.WriteLine($"[info] {cs:yyyy-MM-dd HH:mm}-{ce:HH:mm} sem dados");
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

                                    int sigId;
                                    if (!signalMap.TryGetValue(ch.Id, out sigId))
                                        continue; // canal não existe no catálogo para este pdc_pmu

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
                            Console.WriteLine($"[ok] {cs:yyyy-MM-dd HH:mm}-{ce:HH:mm} inserido");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[erro-chunk] {cs:yyyy-MM-dd HH:mm}-{ce:HH:mm}: {ex.Message}");
                }
            });
        }

        // ----------------- HELPERS (BD / MAPAS) -----------------

        // ===== Helpers =====

        // ===== Helpers =====

        static List<Medi.Ingestor.Gsf.Channel> LoadChannelsFromDb(NpgsqlConnection conn, string source, string pmuIdName)
        {
            const string sql = @"
        SELECT
            s.historian_point,         -- INTEGER no seu schema
            s.name,                    -- TENSAO_A / FREQUENCIA / ...
            s.quantity,                -- 'Voltage' | 'Current' | 'Frequency'
            s.phase,                   -- 'A' | 'B' | 'C' | 'None'
            s.component                -- 'MAG' | 'ANG' | 'FREQ' | 'DFREQ'
        FROM openplot.signal s
        JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
        JOIN openplot.pmu     p   ON p.pmu_id       = ppm.pmu_id
        JOIN openplot.pdc     d   ON d.pdc_id       = ppm.pdc_id
        WHERE d.name = @source
          AND (p.id_name = @pmu OR ppm.pdc_local_id = @pmu)
        ORDER BY s.quantity, s.component, s.phase, s.signal_id;";

            var list = new List<Medi.Ingestor.Gsf.Channel>();

            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@source", source);
                cmd.Parameters.AddWithValue("@pmu", pmuIdName);

                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        // Leitura segura (C# 7.3)
                        var pointId = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
                        var chName = rdr.IsDBNull(1) ? "" : rdr.GetString(1);
                        var qtyStr = rdr.IsDBNull(2) ? "" : rdr.GetString(2);
                        var phaseStr = rdr.IsDBNull(3) ? "" : rdr.GetString(3);
                        var compStr = rdr.IsDBNull(4) ? "" : rdr.GetString(4);

                        var qty = GetQuantityFromDb(qtyStr, compStr);
                        var vtype = GetValueTypeFromDb(qty, compStr);
                        var phase = GetPhaseFromDb(phaseStr);

                        // Para Frequency/DFREQ o ValueType é NONE (valor escalar)
                        list.Add(new Medi.Ingestor.Gsf.Channel(pointId, chName, phase, vtype, qty));
                    }
                }
            }

            return list;
        }

        // ---------- Mapas: DB -> enums usados por Channel ----------

        // Substitui o antigo GetQuantityFromDb(string) por:
        static Medi.Ingestor.Gsf.ChannelQuantity GetQuantityFromDb(string qty, string component)
        {
            if (string.Equals(qty, "Voltage", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelQuantity.VOLTAGE;
            if (string.Equals(qty, "Current", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelQuantity.CURRENT;

            // <- AQUI a diferença: Frequency x DFREQ
            if (string.Equals(qty, "Frequency", StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(component, "DFREQ", StringComparison.OrdinalIgnoreCase))
                    return Medi.Ingestor.Gsf.ChannelQuantity.DFREQ;
                return Medi.Ingestor.Gsf.ChannelQuantity.FREQUENCY; // component = FREQ
            }

            return Medi.Ingestor.Gsf.ChannelQuantity.ANALOG; // fallback seguro
        }

        static Medi.Ingestor.Gsf.ChannelPhase GetPhaseFromDb(string ph)
        {
            if (string.Equals(ph, "A", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelPhase.PHASE_A;
            if (string.Equals(ph, "B", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelPhase.PHASE_B;
            if (string.Equals(ph, "C", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelPhase.PHASE_C;
            return Medi.Ingestor.Gsf.ChannelPhase.NONE;
        }

        static Medi.Ingestor.Gsf.ChannelValueType GetValueTypeFromDb(Medi.Ingestor.Gsf.ChannelQuantity q, string component)
        {
            // Tensões/correntes: MAG/ANG
            if (q == Medi.Ingestor.Gsf.ChannelQuantity.VOLTAGE || q == Medi.Ingestor.Gsf.ChannelQuantity.CURRENT)
            {
                if (string.Equals(component, "MAG", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelValueType.ABSOLUTE;
                if (string.Equals(component, "ANG", StringComparison.OrdinalIgnoreCase)) return Medi.Ingestor.Gsf.ChannelValueType.ANGLE;
            }

            // Freq/DFreq: valor escalar
            return Medi.Ingestor.Gsf.ChannelValueType.NONE;
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


        // pdc + pmu por nome → pdc_pmu_id
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
                        throw new Exception($"Contexto pdc/pmu não encontrado (pdc='{pdcName}', pmu='{pmuIdName}').");
                    return (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2));
                }
            }
        }

        // historian_point (Channel.Id) → signal_id para um pdc_pmu
        private static Dictionary<int, int> LoadSignalMap(NpgsqlConnection conn, int pdcPmuId, IEnumerable<Channel> channels)
        {
            var histIds = channels.Select(c => c.Id).Distinct().ToArray();
            if (histIds.Length == 0) return new Dictionary<int, int>();

            using (var cmd = new NpgsqlCommand(@"
                SELECT historian_point, signal_id
                  FROM openplot.signal
                 WHERE pdc_pmu_id = @pp
                   AND historian_point = ANY(@hids);", conn))
            {
                cmd.Parameters.AddWithValue("pp", pdcPmuId);
                cmd.Parameters.AddWithValue("hids", histIds);

                using (var r = cmd.ExecuteReader())
                {
                    var map = new Dictionary<int, int>();
                    while (r.Read())
                        map[r.GetInt32(0)] = r.GetInt32(1);
                    return map;
                }
            }
        }

        // dedupe meia-aberta [from, to)
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

                    // se cada signal já tem ao menos 1 ponto no intervalo, consideramos presente
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

        // OA (geralmente local no MedPlot) → UTC consistente
        private static DateTime FromOADateUtc(double oa)
        {
            var dtLocal = DateTime.FromOADate(oa);
            return DateTime.SpecifyKind(dtLocal, DateTimeKind.Local).ToUniversalTime();
        }

        private static void LoadConfig()
        {
            PgConnString = ConfigurationManager.AppSettings["Db"];
            XmlPath = ConfigurationManager.AppSettings["MedPlotXml"];
            PollIntervalSeconds = ReadInt("PollIntervalSeconds", 2);
            ChunkMinutes = ReadInt("ChunkMinutes", 10);
            MaxParallelChunks = ReadInt("MaxParallelChunks", 2);

            if (string.IsNullOrWhiteSpace(PgConnString))
                throw new Exception("App.config: defina AppSettings key=Db.");
            if (string.IsNullOrWhiteSpace(XmlPath))
                throw new Exception("App.config: defina AppSettings key=MedPlotXml.");
        }

        private static int ReadInt(string key, int def)
        {
            var v = ConfigurationManager.AppSettings[key];
            return int.TryParse(v, out var n) ? n : def;
        }
    }
}
