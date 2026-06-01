using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using OpenPlot.Ingestor.Gsf.Repository;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal interface IIngestorChunkPipeline
{
    List<Channel> LoadChannels(string source, string pmuIdName);
    bool FetchAndInsert(
        Guid jobId,
        string jobSource,
        SystemData systemCfg,
        Terminal term,
        List<Channel> channels,
        DateTime fromUtc,
        DateTime toUtc,
        int selectRate,
        IngestorProgressReporter progress,
        Func<Guid, bool> isJobCancellationRequested,
        Func<Guid, Exception> createCanceledException);
}

internal sealed class IngestorChunkPipeline : IIngestorChunkPipeline
{
    private readonly IngestorRuntimeContext _runtimeContext;
    private readonly IChunkExecutionCoordinator _chunkExecutionCoordinator;

    public IngestorChunkPipeline(
        IngestorRuntimeContext runtimeContext,
        IChunkExecutionCoordinator chunkExecutionCoordinator)
    {
        _runtimeContext = runtimeContext;
        _chunkExecutionCoordinator = chunkExecutionCoordinator;
    }

    private string PgConnString => _runtimeContext.Options.PgConnString;
    private int ChunkMinutes => _runtimeContext.Options.ChunkMinutes;
    private int MaxParallelChunks => _runtimeContext.Options.MaxParallelChunks;

    public List<Channel> LoadChannels(string source, string pmuIdName)
    {
        using var conn = new NpgsqlConnection(PgConnString);
        conn.Open();
        return LoadChannelsFromDb(conn, source, pmuIdName);
    }

    public bool FetchAndInsert(
        Guid jobId,
        string jobSource,
        SystemData systemCfg,
        Terminal term,
        List<Channel> channels,
        DateTime fromUtc,
        DateTime toUtc,
        int selectRate,
        IngestorProgressReporter progress,
        Func<Guid, bool> isJobCancellationRequested,
        Func<Guid, Exception> createCanceledException)
    {
        var hasData = 0;
        var canceled = 0;

        using var conn = new NpgsqlConnection(PgConnString);
        conn.Open();

        var ctx = GetPdcContext(conn, jobSource, term.Id);
        var pdcPmuId = ctx.pdcPmuId;

        var signalMap = LoadSignalMap(conn, pdcPmuId, channels);
        if (signalMap.Count == 0)
            throw new Exception("Nenhum signal mapeado para os Channel.Id informados (verifique o catálogo).");

        var allSignalIds = signalMap.Values.Distinct().ToArray();
        var totalSpan = toUtc - fromUtc;
        var chunkSize = TimeSpan.FromMinutes(Math.Max(1, ChunkMinutes));
        if (chunkSize > totalSpan)
            chunkSize = totalSpan;

        var intervals = new List<(DateTime cs, DateTime ce)>();
        for (var cs = fromUtc; cs < toUtc; cs = cs.Add(chunkSize))
        {
            var ce = cs.Add(chunkSize);
            if (ce > toUtc)
                ce = toUtc;
            intervals.Add((cs, ce));
        }

        using var cts = new CancellationTokenSource();
        InvalidConnectionException? badConn = null;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, MaxParallelChunks),
            CancellationToken = cts.Token
        };

        try
        {
            Parallel.ForEach(intervals, parallelOptions, (interval, state) =>
            {
                bool CheckCancellation()
                {
                    if (!isJobCancellationRequested(jobId))
                        return false;

                    if (Interlocked.CompareExchange(ref canceled, 1, 0) == 0)
                    {
                        Console.WriteLine("[cancelado] job " + jobId + " interrompido durante processamento.");
                        cts.Cancel();
                        state.Stop();
                    }

                    return true;
                }

                if (parallelOptions.CancellationToken.IsCancellationRequested || CheckCancellation())
                    return;

                var cs = interval.cs;
                var ce = interval.ce;
                IDisposable? lease = null;

                try
                {
                    lease = _chunkExecutionCoordinator.Acquire(parallelOptions.CancellationToken);

                    if (parallelOptions.CancellationToken.IsCancellationRequested || CheckCancellation())
                        return;

                    if (ChunkAlreadyPresentDb(PgConnString, pdcPmuId, allSignalIds, cs, ce))
                    {
                        Console.WriteLine("[skip] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " (já existente)");
                        Interlocked.Exchange(ref hasData, 1);
                        progress.Tick($"Processando: {term.Id}");
                        return;
                    }

                    var repo = RepositoryFactory.Create(systemCfg);
                    var terminalCode = systemCfg.Type == DatabaseType.Medfasee ? term.IdNumber.ToString() : term.Id;

                    var dict = repo.QueryTerminalSeries(
                        terminalCode,
                        cs,
                        ce,
                        channels,
                        selectRate,
                        term.EquipmentRate,
                        false);

                    if (dict == null || dict.Count == 0)
                    {
                        Console.WriteLine("[info] " + cs.ToString("yyyy-MM-dd HH:mm") + "-" + ce.ToString("HH:mm") + " sem dados");
                        progress.Tick($"Processando: {term.Id}");
                        return;
                    }

                    using var connCopy = new NpgsqlConnection(PgConnString);
                    connCopy.Open();
                    using var txCopy = connCopy.BeginTransaction();
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

                    using (var importer = connCopy.BeginBinaryImport(@"
                        COPY measurements_stage_tmp
                        (ts, pdc_pmu_id, signal_id, value)
                        FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var kv in dict)
                        {
                            var ch = kv.Key;
                            var series = kv.Value;
                            if (series == null || series.Count == 0)
                                continue;

                            var key = (ch.Id, ch.Quantity, ch.Phase, ch.Value);
                            if (!signalMap.TryGetValue(key, out var signalId))
                                continue;

                            var ts = series.GetTimestamps();
                            var rd = series.GetReadings();

                            for (var i = 0; i < series.Count; i++)
                            {
                                var dt = FromOADateUtc(ts[i]);
                                var val = rd[i];
                                if (double.IsNaN(val) || double.IsInfinity(val))
                                    continue;
                                if (dt.Year < 1970 || dt.Year > 2100)
                                    continue;

                                importer.StartRow();
                                importer.Write(dt, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                                importer.Write(pdcPmuId, NpgsqlTypes.NpgsqlDbType.Integer);
                                importer.Write(signalId, NpgsqlTypes.NpgsqlDbType.Integer);
                                importer.Write(val, NpgsqlTypes.NpgsqlDbType.Double);
                            }
                        }

                        importer.Complete();
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
                    progress.Tick($"Processando: {term.Id}");
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
                    progress.Tick($"Processando: {term.Id}");
                }
                finally
                {
                    lease?.Dispose();
                }
            });
        }
        catch (OperationCanceledException)
        {
        }

        if (badConn != null)
            throw badConn;

        if (canceled != 0)
            throw createCanceledException(jobId);

        return hasData != 0;
    }

    private static List<Channel> LoadChannelsFromDb(NpgsqlConnection conn, string source, string pmuIdName)
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

        var list = new List<Channel>();
        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@source", source);
        cmd.Parameters.AddWithValue("@pmu", pmuIdName);

        using var rdr = cmd.ExecuteReader();
        while (rdr.Read())
        {
            var pointId = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
            var chName = rdr.IsDBNull(1) ? "" : rdr.GetString(1);
            var qtyStr = rdr.IsDBNull(2) ? "" : rdr.GetString(2);
            var phaseStr = rdr.IsDBNull(3) ? "" : rdr.GetString(3);
            var compStr = rdr.IsDBNull(4) ? "" : rdr.GetString(4);

            var qty = GetQuantityFromDb(qtyStr, compStr);
            var valueType = GetValueTypeFromDb(qty, compStr);
            var phase = GetPhaseFromDb(phaseStr);

            list.Add(new Channel(pointId, chName, phase, valueType, qty));
        }

        return list;
    }

    private static ChannelQuantity GetQuantityFromDb(string qty, string component)
    {
        if (string.Equals(qty, "Voltage", StringComparison.OrdinalIgnoreCase)) return ChannelQuantity.VOLTAGE;
        if (string.Equals(qty, "Current", StringComparison.OrdinalIgnoreCase)) return ChannelQuantity.CURRENT;

        if (string.Equals(qty, "Frequency", StringComparison.OrdinalIgnoreCase))
        {
            if (string.Equals(component, "DFREQ", StringComparison.OrdinalIgnoreCase))
                return ChannelQuantity.DFREQ;
            return ChannelQuantity.FREQUENCY;
        }

        if (string.Equals(qty, "Digital", StringComparison.OrdinalIgnoreCase))
            return ChannelQuantity.DIGITAL;

        return ChannelQuantity.ANALOG;
    }

    private static ChannelPhase GetPhaseFromDb(string ph)
    {
        if (string.Equals(ph, "A", StringComparison.OrdinalIgnoreCase)) return ChannelPhase.PHASE_A;
        if (string.Equals(ph, "B", StringComparison.OrdinalIgnoreCase)) return ChannelPhase.PHASE_B;
        if (string.Equals(ph, "C", StringComparison.OrdinalIgnoreCase)) return ChannelPhase.PHASE_C;
        return ChannelPhase.NONE;
    }

    private static ChannelValueType GetValueTypeFromDb(ChannelQuantity quantity, string component)
    {
        if (quantity == ChannelQuantity.VOLTAGE || quantity == ChannelQuantity.CURRENT)
        {
            if (string.Equals(component, "MAG", StringComparison.OrdinalIgnoreCase)) return ChannelValueType.ABSOLUTE;
            if (string.Equals(component, "ANG", StringComparison.OrdinalIgnoreCase)) return ChannelValueType.ANGLE;
        }

        if (quantity == ChannelQuantity.DIGITAL)
            return ChannelValueType.ABSOLUTE;

        return ChannelValueType.NONE;
    }

    private static (int pdcId, int pmuId, int pdcPmuId) GetPdcContext(NpgsqlConnection conn, string pdcName, string pmuIdName)
    {
        using var cmd = new NpgsqlCommand(@"
            SELECT p.pdc_id, u.pmu_id, pp.pdc_pmu_id
              FROM openplot.pdc        p
              JOIN openplot.pdc_pmu    pp ON pp.pdc_id = p.pdc_id
              JOIN openplot.pmu        u  ON u.pmu_id = pp.pmu_id
             WHERE p.name = @pdc AND u.id_name = @pmu;", conn);
        cmd.Parameters.AddWithValue("pdc", pdcName);
        cmd.Parameters.AddWithValue("pmu", pmuIdName);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            throw new Exception("Contexto pdc/pmu não encontrado (pdc='" + pdcName + "', pmu='" + pmuIdName + "').");

        return (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    private static Dictionary<(int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val), int> LoadSignalMap(
        NpgsqlConnection conn,
        int pdcPmuId,
        IEnumerable<Channel> channels)
    {
        var channelList = channels.ToList();
        if (channelList.Count == 0)
            return new Dictionary<(int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val), int>();

        var histIds = channelList.Select(c => c.Id).Distinct().ToArray();
        if (histIds.Length == 0)
            return new Dictionary<(int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val), int>();

        using var cmd = new NpgsqlCommand(@"
            SELECT
                historian_point,
                quantity::text,
                phase::text,
                component::text,
                signal_id
            FROM openplot.signal
            WHERE pdc_pmu_id = @pp
              AND historian_point = ANY(@hids);", conn);
        cmd.Parameters.AddWithValue("pp", pdcPmuId);
        cmd.Parameters.AddWithValue("hids", histIds);

        using var reader = cmd.ExecuteReader();
        var map = new Dictionary<(int, ChannelQuantity, ChannelPhase, ChannelValueType), int>();
        while (reader.Read())
        {
            var hist = reader.GetInt32(0);
            var qtyStr = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var phaseStr = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var compStr = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var signalId = reader.GetInt32(4);

            var qty = GetQuantityFromDb(qtyStr, compStr);
            var phase = GetPhaseFromDb(phaseStr);
            var valueType = GetValueTypeFromDb(qty, compStr);
            var key = (hist, qty, phase, valueType);

            if (!map.ContainsKey(key))
                map[key] = signalId;
        }

        return map;
    }

    private static bool ChunkAlreadyPresentDb(string connString, int pdcPmuId, int[] signalIds, DateTime from, DateTime to)
    {
        if (signalIds.Length == 0)
            return false;

        using var conn = new NpgsqlConnection(connString);
        conn.Open();
        using var cmd = new NpgsqlCommand(@"
            SELECT COUNT(DISTINCT signal_id)
              FROM openplot.measurements
             WHERE pdc_pmu_id = @pp
               AND signal_id   = ANY(@sids)
               AND ts >= @from AND ts < @to;", conn);
        cmd.Parameters.AddWithValue("pp", pdcPmuId);
        cmd.Parameters.AddWithValue("sids", signalIds);
        cmd.Parameters.AddWithValue("from", from);
        cmd.Parameters.AddWithValue("to", to);

        var countObj = cmd.ExecuteScalar();
        var count = countObj == null || countObj is DBNull ? 0L : Convert.ToInt64(countObj);
        return count >= signalIds.Length;
    }

    private static DateTime FromOADateUtc(double oa)
    {
        var dt = DateTime.FromOADate(oa);
        return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
    }
}
