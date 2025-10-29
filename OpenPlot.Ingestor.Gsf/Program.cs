using Newtonsoft.Json.Linq;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks; // ✅ paralelismo

namespace OpenPlot.Ingestor.Gsf
{
    internal static class Program
    {
        // Configurações globais
        private static string PgConnString;
        private static string XmlPath;
        private static int PollIntervalSeconds;
        private static int ChunkMinutes;
        private static int MaxParallelChunks;   // ✅ novo
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
                    bool found = false;

                    using (var conn = new NpgsqlConnection(PgConnString))
                    {
                        conn.Open();
                        DbOps.EnsureSchema(conn); // ✅ garante schema e índices

                        using (var tx = conn.BeginTransaction())
                        {
                            const string pickSql = @"
                                SELECT id, source, terminal_id, signals::text, from_ts, to_ts, select_rate
                                  FROM medi.search_runs
                                 WHERE status = 'queued'
                                 ORDER BY created_at
                                 FOR UPDATE SKIP LOCKED
                                 LIMIT 1;";

                            Guid id;
                            string source, terminalId, signalsJson;
                            DateTime from, to;
                            int selectRate;

                            using (var cmd = new NpgsqlCommand(pickSql, conn, tx))
                            using (var rdr = cmd.ExecuteReader())
                            {
                                if (!rdr.Read())
                                {
                                    rdr.Close();
                                    tx.Commit();
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
                                    rdr.Close();

                                    DbOps.UpdateStatus(conn, tx, id, "running", 1, "Iniciando");

                                    try
                                    {
                                        var term = TerminalResolver.Resolve(SystemCfg, terminalId);
                                        var signals = ParseSignals(signalsJson);
                                        var channels = TerminalResolver.MapChannels(term, signals);

                                        if (channels.Count == 0)
                                            throw new Exception("Nenhum canal mapeado para os sinais requisitados.");

                                        FetchAndInsert(conn, id, SystemCfg, term, channels, from, to, selectRate);

                                        DbOps.UpdateStatus(conn, tx, id, "done", 100, "Concluído");
                                        tx.Commit();
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"[erro] job {id}: {ex.Message}");
                                        try { tx.Rollback(); } catch { }

                                        using (var tx2 = conn.BeginTransaction())
                                        {
                                            DbOps.UpdateStatus(conn, tx2, id, "failed", 0, ex.Message);
                                            tx2.Commit();
                                        }
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

        // ===================================================
        // PIPELINE PRINCIPAL (AGORA COM PARALELISMO POR CHUNK)
        // ===================================================
        private static void FetchAndInsert(
    NpgsqlConnection conn,
    Guid jobId,
    SystemData systemCfg,
    Terminal term,
    List<Channel> channels,
    DateTime fromUtc,
    DateTime toUtc,
    int selectRate)
        {
            // ❌ REMOVA esta linha (C# 7.3 não suporta tipo inferido p/ delegate)
            // var repoFactory = RepositoryFactory.Create;

            var chunkSize = TimeSpan.FromMinutes(ChunkMinutes);
            if (chunkSize > (toUtc - fromUtc)) chunkSize = (toUtc - fromUtc);

            // ✅ use tupla corretamente
            var intervals = new List<(DateTime cs, DateTime ce)>();
            for (var cs = fromUtc; cs < toUtc; cs = cs.Add(chunkSize).AddMilliseconds(1))
            {
                var ce = cs.Add(chunkSize) < toUtc ? cs.Add(chunkSize) : toUtc;
                if (ce < toUtc) ce = ce.AddMilliseconds(-1);
                intervals.Add((cs, ce));
            }

            var po = new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, MaxParallelChunks) };

            Parallel.ForEach(intervals, po, interval =>
            {
                var cs = interval.cs;
                var ce = interval.ce;

                try
                {
                    if (ChunkAlreadyPresentDb(PgConnString, channels, cs, ce))
                    {
                        Console.WriteLine($"[skip] {cs:HH:mm}-{ce:HH:mm} (já existente)");
                        return;
                    }

                    // ✅ chame o factory diretamente aqui
                    var repo = RepositoryFactory.Create(systemCfg);

                    var dict = repo.QueryTerminalSeries(term.Id, cs, ce, channels, selectRate, term.EquipmentRate, false);
                    if (dict == null || dict.Count == 0)
                    {
                        Console.WriteLine($"[info] {cs:HH:mm}-{ce:HH:mm} sem dados");
                        return;
                    }

                    using (var connCopy = new NpgsqlConnection(PgConnString))
                    {
                        connCopy.Open();
                        using (var txCopy = connCopy.BeginTransaction())
                        {
                            using (var cmd = new NpgsqlCommand(@"
                        CREATE TEMP TABLE IF NOT EXISTS measurements_stage_tmp (
                            ts            timestamptz NOT NULL,
                            signal_id     text        NOT NULL,
                            historian_id  integer     NOT NULL,
                            value         double precision NOT NULL,
                            region        text NULL,
                            substation    text NULL
                        ) ON COMMIT DROP;
                        TRUNCATE measurements_stage_tmp;", connCopy, txCopy))
                            {
                                cmd.ExecuteNonQuery();
                            }

                            using (var imp = connCopy.BeginBinaryImport(@"
                        COPY measurements_stage_tmp
                        (ts, signal_id, historian_id, value, region, substation)
                        FROM STDIN (FORMAT BINARY)"))
                            {
                                foreach (var kv in dict)
                                {
                                    var ch = kv.Key;
                                    var ts = kv.Value.GetTimestamps();
                                    var rd = kv.Value.GetReadings();
                                    if (kv.Value == null || kv.Value.Count == 0) continue;

                                    var sid = ResolveSignalId(ch);
                                    var reg = (object)term.Area ?? DBNull.Value;
                                    var sub = (object)term.Station ?? DBNull.Value;
                                    int hid = ch.Id;

                                    for (int i = 0; i < kv.Value.Count; i++)
                                    {
                                        var dt = FromOADateUtc(ts[i]);
                                        var val = rd[i];
                                        if (double.IsNaN(val) || double.IsInfinity(val)) continue;
                                        if (dt.Year < 1970 || dt.Year > 2100) continue;

                                        imp.StartRow();
                                        imp.Write(dt, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                                        imp.Write(sid, NpgsqlTypes.NpgsqlDbType.Text);
                                        imp.Write(hid, NpgsqlTypes.NpgsqlDbType.Integer);
                                        imp.Write(val, NpgsqlTypes.NpgsqlDbType.Double);
                                        imp.Write(reg, NpgsqlTypes.NpgsqlDbType.Text);
                                        imp.Write(sub, NpgsqlTypes.NpgsqlDbType.Text);
                                    }
                                }
                                imp.Complete();
                            }

                            using (var upsert = new NpgsqlCommand(@"
                        INSERT INTO medi.measurements (ts, signal_id, historian_id, value, region, substation)
                        SELECT ts, signal_id, historian_id, value, region, substation
                          FROM measurements_stage_tmp
                        ON CONFLICT (signal_id, historian_id, ts) DO NOTHING;", connCopy, txCopy))
                            {
                                upsert.ExecuteNonQuery();
                            }

                            txCopy.Commit();
                        }
                    }

                    Console.WriteLine($"[ok] {cs:HH:mm}-{ce:HH:mm} inserido");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[erro-chunk] {cs:HH:mm}-{ce:HH:mm}: {ex.Message}");
                }
            });
        }


        // ===================================================
        // FUNÇÕES AUXILIARES
        // ===================================================

        // Versão thread-safe do dedupe (cada thread usa sua própria conexão)
        private static bool ChunkAlreadyPresentDb(string connString, List<Channel> channels, DateTime from, DateTime to)
        {
            var ids = channels.Select(c => c.Id).Distinct().ToArray();
            using (var c = new NpgsqlConnection(connString))
            {
                c.Open();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT COUNT(DISTINCT historian_id)
                      FROM medi.measurements
                     WHERE historian_id = ANY(@hids)
                       AND ts BETWEEN @from AND @to;", c))
                {
                    cmd.Parameters.AddWithValue("hids", ids);
                    cmd.Parameters.AddWithValue("from", from);
                    cmd.Parameters.AddWithValue("to", to);
                    var count = (long)cmd.ExecuteScalar();
                    return count >= Math.Min(ids.Length, channels.Count);
                }
            }
        }

        private static List<string> ParseSignals(string jsonArray)
        {
            if (string.IsNullOrWhiteSpace(jsonArray))
                return new List<string>();
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

        private static string ResolveSignalId(Channel ch) =>
            !string.IsNullOrWhiteSpace(ch.Name) ? ch.Name : ch.Id.ToString();

        private static DateTime FromOADateUtc(double oa)
        {
            var dt = DateTime.FromOADate(oa);
            return dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime();
        }

        private static void LoadConfig()
        {
            PgConnString = System.Configuration.ConfigurationManager.AppSettings["Db"];
            XmlPath = System.Configuration.ConfigurationManager.AppSettings["MedPlotXml"];
            PollIntervalSeconds = ReadInt("PollIntervalSeconds", 2);
            ChunkMinutes = ReadInt("ChunkMinutes", 10);
            MaxParallelChunks = ReadInt("MaxParallelChunks", 2); // ✅ novo

            if (string.IsNullOrWhiteSpace(PgConnString))
                throw new Exception("App.config: defina AppSettings key=Db.");
            if (string.IsNullOrWhiteSpace(XmlPath))
                throw new Exception("App.config: defina AppSettings key=MedPlotXml.");
        }

        private static int ReadInt(string key, int def)
        {
            var v = System.Configuration.ConfigurationManager.AppSettings[key];
            return int.TryParse(v, out var n) ? n : def;
        }
    }
}

