using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using OpenPlot.Ingestor.Gsf.Data;
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

    // Janela usada somente para obter o last_valid antes do início do chunk.
    // Os dados anteriores a cs NÃO são persistidos novamente.
    private const int HoldLastLookbackSeconds = 2;

    // Um frame real é associado ao frame nominal mais próximo somente se estiver
    // dentro desta fração do período de amostragem.
    //
    // 0.45 => tolerância de ±45% de um frame:
    //   30 fps  -> ±15.000 ms
    //   60 fps  -> ±7.500 ms
    //   120 fps -> ±3.750 ms
    //
    // Mantém folga sem ultrapassar metade do período, evitando ambiguidade
    // com o frame vizinho.
    private const double FrameToleranceFraction = 0.45;

    private readonly record struct SignalSample(
        DateTime Ts,
        double Value,
        int Quality);

    private readonly record struct MappedFrameSample(
        SignalSample Sample,
        long DistanceTicks);

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
        if (selectRate <= 0)
            throw new ArgumentOutOfRangeException(nameof(selectRate), "selectRate deve ser maior que zero.");

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
                        Console.WriteLine(
                            "[skip] " + cs.ToString("yyyy-MM-dd HH:mm") +
                            "-" + ce.ToString("HH:mm") + " (já existente)");
                        Interlocked.Exchange(ref hasData, 1);
                        progress.Tick($"Processando: {term.Id}");
                        return;
                    }

                    var repo = RepositoryFactory.Create(systemCfg);
                    var terminalCode =
                        systemCfg.Type == DatabaseType.Medfasee
                            ? term.IdNumber.ToString()
                            : term.Id;

                    // Busca um pequeno trecho antes de cs para obter o último valor real
                    // de cada sinal. Esse lookback é usado exclusivamente pelo hold-last.
                    var queryFrom = cs.AddSeconds(-HoldLastLookbackSeconds);

                    var dict = repo.QueryTerminalSeries(
                        terminalCode,
                        queryFrom,
                        ce,
                        channels,
                        selectRate,
                        term.EquipmentRate,
                        false);

                    if (dict == null || dict.Count == 0)
                    {
                        Console.WriteLine(
                            "[info] " + cs.ToString("yyyy-MM-dd HH:mm") +
                            "-" + ce.ToString("HH:mm") + " sem dados");
                        progress.Tick($"Processando: {term.Id}");
                        return;
                    }

                    // Organiza os dados reais por signal_id.
                    // Isso é essencial: frame presente em um sinal NÃO implica frame
                    // presente nos demais sinais.
                    var samplesBySignal = BuildSignalSamples(dict, signalMap);

                    // Para cada signal_id, associa cada amostra real ao frame nominal
                    // mais próximo, respeitando a tolerância explícita.
                    var receivedFramesBySignal =
                        BuildReceivedFramesBySignal(samplesBySignal, selectRate);

                    using var connCopy = new NpgsqlConnection(PgConnString);
                    connCopy.Open();
                    using var txCopy = connCopy.BeginTransaction();

                    using (var cmd = new NpgsqlCommand(@"
                        CREATE TEMP TABLE IF NOT EXISTS measurements_wide_stage_tmp (
                            ts              timestamptz       NOT NULL,
                            pdc_pmu_id      integer           NOT NULL,
                            quality         integer           NULL,
                            va_mod_v        double precision  NULL,
                            va_ang_deg      double precision  NULL,
                            vb_mod_v        double precision  NULL,
                            vb_ang_deg      double precision  NULL,
                            vc_mod_v        double precision  NULL,
                            vc_ang_deg      double precision  NULL,
                            ia_mod_a        double precision  NULL,
                            ia_ang_deg      double precision  NULL,
                            ib_mod_a        double precision  NULL,
                            ib_ang_deg      double precision  NULL,
                            ic_mod_a        double precision  NULL,
                            ic_ang_deg      double precision  NULL,
                            cthd_a_pct      double precision  NULL,
                            cthd_b_pct      double precision  NULL,
                            cthd_c_pct      double precision  NULL,
                            vthd_a_pct      double precision  NULL,
                            vthd_b_pct      double precision  NULL,
                            vthd_c_pct      double precision  NULL,
                            frequency_hz    double precision  NULL,
                            delta_freq_hz   double precision  NULL,
                            cfds            double precision  NULL
                        ) ON COMMIT DROP;
                        TRUNCATE measurements_wide_stage_tmp;", connCopy, txCopy))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    var distinctSignalIds = signalMap.Values.Distinct().ToArray();
                    var wideColumnBySignal = LoadWideColumnBySignalId(connCopy, pdcPmuId, distinctSignalIds);
                    var missingFilled = 0;
                    var missingWithoutLastValid = 0;

                    var stagedFrames = new Dictionary<(DateTime Ts, int PdcPmuId), WideFrameRow>();

                    // ============================================================
                    // 1) Frames reais recebidos do historiador
                    // ============================================================
                    foreach (var sid in distinctSignalIds)
                    {
                        if (!wideColumnBySignal.TryGetValue(sid, out var wideColumn))
                            continue;

                        if (!samplesBySignal.TryGetValue(sid, out var samples))
                            continue;

                        foreach (var sample in samples)
                        {
                            if (sample.Ts < cs || sample.Ts >= ce)
                                continue;

                            var row = GetOrCreateWideFrame(stagedFrames, sample.Ts, pdcPmuId);
                            SetWideValue(row, wideColumn, sample.Value);
                            row.Quality = MergeWideFrameQuality(row.Quality, sample.Quality, false);
                        }
                    }

                    // ============================================================
                    // 2) Frames faltantes -> HOLD-LAST por signal_id
                    // ============================================================
                    foreach (var sid in distinctSignalIds)
                    {
                        if (!wideColumnBySignal.TryGetValue(sid, out var wideColumn))
                            continue;

                        SignalSample? lastValid = null;

                        if (samplesBySignal.TryGetValue(sid, out var samples))
                        {
                            foreach (var sample in samples)
                            {
                                if (sample.Ts >= cs)
                                    break;

                                lastValid = sample;
                            }
                        }

                        receivedFramesBySignal.TryGetValue(
                            sid,
                            out var receivedFrames);

                        foreach (var expectedTs in EnumerateExpectedFrames(cs, ce, selectRate))
                        {
                            var frameKey = GetExpectedFrameKey(expectedTs, selectRate);

                            if (receivedFrames != null &&
                                receivedFrames.TryGetValue(frameKey, out var mapped))
                            {
                                lastValid = mapped.Sample;
                                continue;
                            }

                            if (lastValid.HasValue)
                            {
                                var row = GetOrCreateWideFrame(stagedFrames, expectedTs, pdcPmuId);
                                SetWideValue(row, wideColumn, lastValid.Value.Value);
                                row.Quality = MergeWideFrameQuality(row.Quality, 2, true);
                                missingFilled++;
                            }
                            else
                            {
                                missingWithoutLastValid++;
                            }
                        }
                    }

                    using (var importer = connCopy.BeginBinaryImport(@"
                        COPY measurements_wide_stage_tmp
                        (ts, pdc_pmu_id, quality,
                         va_mod_v, va_ang_deg, vb_mod_v, vb_ang_deg, vc_mod_v, vc_ang_deg,
                         ia_mod_a, ia_ang_deg, ib_mod_a, ib_ang_deg, ic_mod_a, ic_ang_deg,
                         cthd_a_pct, cthd_b_pct, cthd_c_pct,
                         vthd_a_pct, vthd_b_pct, vthd_c_pct,
                         frequency_hz, delta_freq_hz, cfds)
                        FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var frame in stagedFrames.Values.OrderBy(x => x.Ts))
                        {
                            importer.StartRow();
                            importer.Write(frame.Ts, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                            importer.Write(frame.PdcPmuId, NpgsqlTypes.NpgsqlDbType.Integer);
                            if (frame.Quality.HasValue)
                                importer.Write(frame.Quality.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                            else
                                importer.WriteNull();

                            WriteNullableDouble(importer, frame.VaModV);
                            WriteNullableDouble(importer, frame.VaAngDeg);
                            WriteNullableDouble(importer, frame.VbModV);
                            WriteNullableDouble(importer, frame.VbAngDeg);
                            WriteNullableDouble(importer, frame.VcModV);
                            WriteNullableDouble(importer, frame.VcAngDeg);
                            WriteNullableDouble(importer, frame.IaModA);
                            WriteNullableDouble(importer, frame.IaAngDeg);
                            WriteNullableDouble(importer, frame.IbModA);
                            WriteNullableDouble(importer, frame.IbAngDeg);
                            WriteNullableDouble(importer, frame.IcModA);
                            WriteNullableDouble(importer, frame.IcAngDeg);
                            WriteNullableDouble(importer, frame.CthdAPct);
                            WriteNullableDouble(importer, frame.CthdBPct);
                            WriteNullableDouble(importer, frame.CthdCPct);
                            WriteNullableDouble(importer, frame.VthdAPct);
                            WriteNullableDouble(importer, frame.VthdBPct);
                            WriteNullableDouble(importer, frame.VthdCPct);
                            WriteNullableDouble(importer, frame.FrequencyHz);
                            WriteNullableDouble(importer, frame.DeltaFreqHz);
                            WriteNullableDouble(importer, frame.Cfds);
                        }

                        importer.Complete();
                    }

                    using (var upsert = new NpgsqlCommand(@"
                        INSERT INTO openplot.measurements_wide
                            (ts, pdc_pmu_id, quality,
                             va_mod_v, va_ang_deg, vb_mod_v, vb_ang_deg, vc_mod_v, vc_ang_deg,
                             ia_mod_a, ia_ang_deg, ib_mod_a, ib_ang_deg, ic_mod_a, ic_ang_deg,
                             cthd_a_pct, cthd_b_pct, cthd_c_pct,
                             vthd_a_pct, vthd_b_pct, vthd_c_pct,
                             frequency_hz, delta_freq_hz, cfds)
                        SELECT
                            ts, pdc_pmu_id, quality,
                            va_mod_v, va_ang_deg, vb_mod_v, vb_ang_deg, vc_mod_v, vc_ang_deg,
                            ia_mod_a, ia_ang_deg, ib_mod_a, ib_ang_deg, ic_mod_a, ic_ang_deg,
                            cthd_a_pct, cthd_b_pct, cthd_c_pct,
                            vthd_a_pct, vthd_b_pct, vthd_c_pct,
                            frequency_hz, delta_freq_hz, cfds
                        FROM measurements_wide_stage_tmp
                        ON CONFLICT (pdc_pmu_id, ts) DO UPDATE
                        SET
                            quality = CASE
                                WHEN openplot.measurements_wide.quality = 2 OR EXCLUDED.quality = 2 THEN 2
                                ELSE COALESCE(EXCLUDED.quality, openplot.measurements_wide.quality)
                            END,
                            va_mod_v      = COALESCE(EXCLUDED.va_mod_v, openplot.measurements_wide.va_mod_v),
                            va_ang_deg    = COALESCE(EXCLUDED.va_ang_deg, openplot.measurements_wide.va_ang_deg),
                            vb_mod_v      = COALESCE(EXCLUDED.vb_mod_v, openplot.measurements_wide.vb_mod_v),
                            vb_ang_deg    = COALESCE(EXCLUDED.vb_ang_deg, openplot.measurements_wide.vb_ang_deg),
                            vc_mod_v      = COALESCE(EXCLUDED.vc_mod_v, openplot.measurements_wide.vc_mod_v),
                            vc_ang_deg    = COALESCE(EXCLUDED.vc_ang_deg, openplot.measurements_wide.vc_ang_deg),
                            ia_mod_a      = COALESCE(EXCLUDED.ia_mod_a, openplot.measurements_wide.ia_mod_a),
                            ia_ang_deg    = COALESCE(EXCLUDED.ia_ang_deg, openplot.measurements_wide.ia_ang_deg),
                            ib_mod_a      = COALESCE(EXCLUDED.ib_mod_a, openplot.measurements_wide.ib_mod_a),
                            ib_ang_deg    = COALESCE(EXCLUDED.ib_ang_deg, openplot.measurements_wide.ib_ang_deg),
                            ic_mod_a      = COALESCE(EXCLUDED.ic_mod_a, openplot.measurements_wide.ic_mod_a),
                            ic_ang_deg    = COALESCE(EXCLUDED.ic_ang_deg, openplot.measurements_wide.ic_ang_deg),
                            cthd_a_pct    = COALESCE(EXCLUDED.cthd_a_pct, openplot.measurements_wide.cthd_a_pct),
                            cthd_b_pct    = COALESCE(EXCLUDED.cthd_b_pct, openplot.measurements_wide.cthd_b_pct),
                            cthd_c_pct    = COALESCE(EXCLUDED.cthd_c_pct, openplot.measurements_wide.cthd_c_pct),
                            vthd_a_pct    = COALESCE(EXCLUDED.vthd_a_pct, openplot.measurements_wide.vthd_a_pct),
                            vthd_b_pct    = COALESCE(EXCLUDED.vthd_b_pct, openplot.measurements_wide.vthd_b_pct),
                            vthd_c_pct    = COALESCE(EXCLUDED.vthd_c_pct, openplot.measurements_wide.vthd_c_pct),
                            frequency_hz  = COALESCE(EXCLUDED.frequency_hz, openplot.measurements_wide.frequency_hz),
                            delta_freq_hz = COALESCE(EXCLUDED.delta_freq_hz, openplot.measurements_wide.delta_freq_hz),
                            cfds          = COALESCE(EXCLUDED.cfds, openplot.measurements_wide.cfds);",
                        connCopy,
                        txCopy))
                    {
                        upsert.ExecuteNonQuery();
                    }

                    txCopy.Commit();
                    Interlocked.Exchange(ref hasData, 1);

                    Console.WriteLine(
                        "[ok] " + term.Id + " " +
                        cs.ToString("yyyy-MM-dd HH:mm") + "-" +
                        ce.ToString("HH:mm") +
                        " inserido | hold-last=" + missingFilled +
                        " | sem-last-valid=" + missingWithoutLastValid);

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
                    Console.WriteLine(
                        "[erro-chunk] " + term.Id + " " +
                        cs.ToString("yyyy-MM-dd HH:mm") + "-" +
                        ce.ToString("HH:mm") + ": " + ex.Message);

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

    private static List<Channel> LoadChannelsFromDb(
        NpgsqlConnection conn,
        string source,
        string pmuIdName)
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

    private static ChannelQuantity GetQuantityFromDb(
        string qty,
        string component)
    {
        if (string.Equals(qty, "Voltage", StringComparison.OrdinalIgnoreCase))
            return ChannelQuantity.VOLTAGE;

        if (string.Equals(qty, "Current", StringComparison.OrdinalIgnoreCase))
            return ChannelQuantity.CURRENT;

        if (string.Equals(qty, "Frequency", StringComparison.OrdinalIgnoreCase))
        {
            if (string.Equals(
                    component,
                    "DFREQ",
                    StringComparison.OrdinalIgnoreCase))
            {
                return ChannelQuantity.DFREQ;
            }

            return ChannelQuantity.FREQUENCY;
        }

        if (string.Equals(qty, "Digital", StringComparison.OrdinalIgnoreCase))
            return ChannelQuantity.DIGITAL;

        return ChannelQuantity.ANALOG;
    }

    private static ChannelPhase GetPhaseFromDb(string ph)
    {
        if (string.Equals(ph, "A", StringComparison.OrdinalIgnoreCase))
            return ChannelPhase.PHASE_A;

        if (string.Equals(ph, "B", StringComparison.OrdinalIgnoreCase))
            return ChannelPhase.PHASE_B;

        if (string.Equals(ph, "C", StringComparison.OrdinalIgnoreCase))
            return ChannelPhase.PHASE_C;

        return ChannelPhase.NONE;
    }

    private static ChannelValueType GetValueTypeFromDb(
        ChannelQuantity quantity,
        string component)
    {
        if (quantity == ChannelQuantity.VOLTAGE ||
            quantity == ChannelQuantity.CURRENT)
        {
            if (string.Equals(
                    component,
                    "MAG",
                    StringComparison.OrdinalIgnoreCase))
            {
                return ChannelValueType.ABSOLUTE;
            }

            if (string.Equals(
                    component,
                    "ANG",
                    StringComparison.OrdinalIgnoreCase))
            {
                return ChannelValueType.ANGLE;
            }
        }

        if (quantity == ChannelQuantity.DIGITAL)
            return ChannelValueType.ABSOLUTE;

        return ChannelValueType.NONE;
    }

    private static (
        int pdcId,
        int pmuId,
        int pdcPmuId) GetPdcContext(
        NpgsqlConnection conn,
        string pdcName,
        string pmuIdName)
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
        {
            throw new Exception(
                "Contexto pdc/pmu não encontrado (pdc='" +
                pdcName + "', pmu='" + pmuIdName + "').");
        }

        return (
            reader.GetInt32(0),
            reader.GetInt32(1),
            reader.GetInt32(2));
    }

    private static Dictionary<
        (int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val),
        int> LoadSignalMap(
        NpgsqlConnection conn,
        int pdcPmuId,
        IEnumerable<Channel> channels)
    {
        var channelList = channels.ToList();
        if (channelList.Count == 0)
        {
            return new Dictionary<
                (int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val),
                int>();
        }

        var histIds = channelList
            .Select(c => c.Id)
            .Distinct()
            .ToArray();

        if (histIds.Length == 0)
        {
            return new Dictionary<
                (int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val),
                int>();
        }

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

        var map = new Dictionary<
            (int, ChannelQuantity, ChannelPhase, ChannelValueType),
            int>();

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

    private static Dictionary<int, string> LoadWideColumnBySignalId(
        NpgsqlConnection conn,
        int pdcPmuId,
        int[] signalIds)
    {
        var result = new Dictionary<int, string>();

        if (signalIds.Length == 0)
            return result;

        using var cmd = new NpgsqlCommand(@"
            SELECT signal_id, quantity::text, phase::text, component::text, COALESCE(name,'')
              FROM openplot.signal
             WHERE pdc_pmu_id = @pp
               AND signal_id = ANY(@sids);", conn);

        cmd.Parameters.AddWithValue("pp", pdcPmuId);
        cmd.Parameters.AddWithValue("sids", signalIds);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var signalId = reader.GetInt32(0);
            var quantity = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var phase = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var component = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var name = reader.IsDBNull(4) ? "" : reader.GetString(4);

            var wideColumn = ResolveWideColumn(quantity, phase, component, name);
            if (!string.IsNullOrWhiteSpace(wideColumn))
                result[signalId] = wideColumn;
        }

        return result;
    }

    private static string ResolveWideColumn(
        string quantity,
        string phase,
        string component,
        string signalName)
    {
        var q = (quantity ?? "").Trim().ToLowerInvariant();
        var p = (phase ?? "").Trim().ToUpperInvariant();
        var c = (component ?? "").Trim().ToUpperInvariant();
        var n = (signalName ?? "").Trim().ToUpperInvariant();

        if (q is "voltage" or "v")
        {
            if (p == "A" && c == "MAG") return "va_mod_v";
            if (p == "A" && c == "ANG") return "va_ang_deg";
            if (p == "B" && c == "MAG") return "vb_mod_v";
            if (p == "B" && c == "ANG") return "vb_ang_deg";
            if (p == "C" && c == "MAG") return "vc_mod_v";
            if (p == "C" && c == "ANG") return "vc_ang_deg";
            if (p == "A" && c == "THD") return "vthd_a_pct";
            if (p == "B" && c == "THD") return "vthd_b_pct";
            if (p == "C" && c == "THD") return "vthd_c_pct";
        }

        if (q is "current" or "i")
        {
            if (p == "A" && c == "MAG") return "ia_mod_a";
            if (p == "A" && c == "ANG") return "ia_ang_deg";
            if (p == "B" && c == "MAG") return "ib_mod_a";
            if (p == "B" && c == "ANG") return "ib_ang_deg";
            if (p == "C" && c == "MAG") return "ic_mod_a";
            if (p == "C" && c == "ANG") return "ic_ang_deg";
            if (p == "A" && c == "THD") return "cthd_a_pct";
            if (p == "B" && c == "THD") return "cthd_b_pct";
            if (p == "C" && c == "THD") return "cthd_c_pct";
        }

        if (q is "frequency" or "freq")
        {
            if (c == "FREQ") return "frequency_hz";
            if (c == "DFREQ") return "delta_freq_hz";
        }

        if (q is "digital" or "d")
        {
            if (c == "DIG" && n == "CFDS")
                return "cfds";
        }

        return string.Empty;
    }

    private sealed class WideFrameRow
    {
        public DateTime Ts { get; init; }
        public int PdcPmuId { get; init; }
        public int? Quality { get; set; }

        public double? VaModV { get; set; }
        public double? VaAngDeg { get; set; }
        public double? VbModV { get; set; }
        public double? VbAngDeg { get; set; }
        public double? VcModV { get; set; }
        public double? VcAngDeg { get; set; }

        public double? IaModA { get; set; }
        public double? IaAngDeg { get; set; }
        public double? IbModA { get; set; }
        public double? IbAngDeg { get; set; }
        public double? IcModA { get; set; }
        public double? IcAngDeg { get; set; }

        public double? CthdAPct { get; set; }
        public double? CthdBPct { get; set; }
        public double? CthdCPct { get; set; }

        public double? VthdAPct { get; set; }
        public double? VthdBPct { get; set; }
        public double? VthdCPct { get; set; }

        public double? FrequencyHz { get; set; }
        public double? DeltaFreqHz { get; set; }
        public double? Cfds { get; set; }
    }

    private static WideFrameRow GetOrCreateWideFrame(
        Dictionary<(DateTime Ts, int PdcPmuId), WideFrameRow> frames,
        DateTime ts,
        int pdcPmuId)
    {
        var key = (ts, pdcPmuId);
        if (!frames.TryGetValue(key, out var row))
        {
            row = new WideFrameRow
            {
                Ts = ts,
                PdcPmuId = pdcPmuId
            };
            frames[key] = row;
        }

        return row;
    }

    private static int? MergeWideFrameQuality(int? current, int incoming, bool isHoldLast)
    {
        if (isHoldLast || incoming == 2)
            return 2;

        if (!current.HasValue)
            return incoming;

        if (current.Value == 2)
            return 2;

        return Math.Min(current.Value, incoming);
    }

    private static void SetWideValue(WideFrameRow row, string wideColumn, double value)
    {
        switch (wideColumn)
        {
            case "va_mod_v": row.VaModV ??= value; break;
            case "va_ang_deg": row.VaAngDeg ??= value; break;
            case "vb_mod_v": row.VbModV ??= value; break;
            case "vb_ang_deg": row.VbAngDeg ??= value; break;
            case "vc_mod_v": row.VcModV ??= value; break;
            case "vc_ang_deg": row.VcAngDeg ??= value; break;

            case "ia_mod_a": row.IaModA ??= value; break;
            case "ia_ang_deg": row.IaAngDeg ??= value; break;
            case "ib_mod_a": row.IbModA ??= value; break;
            case "ib_ang_deg": row.IbAngDeg ??= value; break;
            case "ic_mod_a": row.IcModA ??= value; break;
            case "ic_ang_deg": row.IcAngDeg ??= value; break;

            case "cthd_a_pct": row.CthdAPct ??= value; break;
            case "cthd_b_pct": row.CthdBPct ??= value; break;
            case "cthd_c_pct": row.CthdCPct ??= value; break;

            case "vthd_a_pct": row.VthdAPct ??= value; break;
            case "vthd_b_pct": row.VthdBPct ??= value; break;
            case "vthd_c_pct": row.VthdCPct ??= value; break;

            case "frequency_hz": row.FrequencyHz ??= value; break;
            case "delta_freq_hz": row.DeltaFreqHz ??= value; break;
            case "cfds": row.Cfds ??= value; break;
        }
    }

    private static void WriteNullableDouble(NpgsqlBinaryImporter importer, double? value)
    {
        if (value.HasValue)
            importer.Write(value.Value, NpgsqlTypes.NpgsqlDbType.Double);
        else
            importer.WriteNull();
    }

    private static bool ChunkAlreadyPresentDb(
        string connString,
        int pdcPmuId,
        int[] signalIds,
        DateTime from,
        DateTime to)
    {
        if (signalIds.Length == 0)
            return false;

        using var conn = new NpgsqlConnection(connString);
        conn.Open();

        var wideMap = LoadWideColumnBySignalId(conn, pdcPmuId, signalIds);
        var columns = wideMap.Values
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (columns.Length == 0)
            return false;

        foreach (var column in columns)
        {
            using var cmd = new NpgsqlCommand($@"
                SELECT 1
                  FROM openplot.measurements_wide mw
                 WHERE mw.pdc_pmu_id = @pp
                   AND mw.ts >= @from
                   AND mw.ts <  @to
                   AND mw.{column} IS NOT NULL
                 LIMIT 1;", conn);

            cmd.Parameters.AddWithValue("pp", pdcPmuId);
            cmd.Parameters.AddWithValue("from", from);
            cmd.Parameters.AddWithValue("to", to);

            var exists = cmd.ExecuteScalar() != null;
            if (!exists)
                return false;
        }

        return true;
    }

    private static DateTime FromOADateUtc(double oa)
    {
        var dt = DateTime.FromOADate(oa);
        return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
    }

    /// <summary>
    /// Converte o dicionário retornado pelo historiador em séries agrupadas por signal_id.
    /// Mantém somente valores numéricos válidos e ordena cada série por timestamp.
    /// </summary>
    private static Dictionary<int, List<SignalSample>> BuildSignalSamples(
        Dictionary<Channel, ITimeSeries> dict,
        Dictionary<
            (int hist, ChannelQuantity qty, ChannelPhase phase, ChannelValueType val),
            int> signalMap)
    {
        var result = new Dictionary<int, List<SignalSample>>();

        foreach (var kv in dict)
        {
            var ch = kv.Key;
            var series = kv.Value;

            if (series == null || series.Count == 0)
                continue;

            var channelKey = (
                ch.Id,
                ch.Quantity,
                ch.Phase,
                ch.Value);

            if (!signalMap.TryGetValue(channelKey, out var signalId))
                continue;

            if (!result.TryGetValue(signalId, out var samples))
            {
                samples = new List<SignalSample>(series.Count);
                result[signalId] = samples;
            }

            var ts = series.GetTimestamps();
            var rd = series.GetReadings();
            var ql = series.GetQualities();

            for (var i = 0; i < series.Count; i++)
            {
                var dt = FromOADateUtc(ts[i]);
                var val = rd[i];
                var quality = ql[i];

                if (double.IsNaN(val) || double.IsInfinity(val))
                    continue;

                if (dt.Year < 1970 || dt.Year > 2100)
                    continue;

                samples.Add(new SignalSample(dt, val, quality));
            }
        }

        foreach (var samples in result.Values)
            samples.Sort((a, b) => a.Ts.CompareTo(b.Ts));

        return result;
    }

    /// <summary>
    /// Para cada signal_id, cria o mapa frame-nominal -> amostra real.
    ///
    /// A associação é individual por sinal e usa tolerância explícita.
    /// Se duas amostras caírem no mesmo frame, mantém a mais próxima do
    /// timestamp nominal daquele frame.
    /// </summary>
    private static Dictionary<
        int,
        Dictionary<(long SecTicks, int FrameIdx), MappedFrameSample>>
        BuildReceivedFramesBySignal(
            Dictionary<int, List<SignalSample>> samplesBySignal,
            int selectRate)
    {
        var result = new Dictionary<
            int,
            Dictionary<(long SecTicks, int FrameIdx), MappedFrameSample>>();

        foreach (var kv in samplesBySignal)
        {
            var signalId = kv.Key;
            var samples = kv.Value;

            var frames =
                new Dictionary<
                    (long SecTicks, int FrameIdx),
                    MappedFrameSample>();

            foreach (var sample in samples)
            {
                if (!TryMapTimestampToFrame(
                        sample.Ts,
                        selectRate,
                        out var frameKey,
                        out _,
                        out var distanceTicks))
                {
                    continue;
                }

                var mapped = new MappedFrameSample(
                    sample,
                    distanceTicks);

                if (!frames.TryGetValue(frameKey, out var existing) ||
                    mapped.DistanceTicks < existing.DistanceTicks)
                {
                    frames[frameKey] = mapped;
                }
            }

            result[signalId] = frames;
        }

        return result;
    }

    /// <summary>
    /// Associa um timestamp real ao frame nominal mais próximo.
    ///
    /// Diferente da versão anterior:
    /// - usa ticks, preservando precisão sub-milisegundo;
    /// - a tolerância é explícita;
    /// - não depende de DateTime.Millisecond;
    /// - não considera automaticamente qualquer valor arredondado como válido.
    /// </summary>
    private static bool TryMapTimestampToFrame(
        DateTime dt,
        int selectRate,
        out (long SecTicks, int FrameIdx) frameKey,
        out DateTime expectedTs,
        out long distanceTicks)
    {
        if (selectRate <= 0)
            throw new ArgumentOutOfRangeException(nameof(selectRate));

        var utc =
            dt.Kind == DateTimeKind.Utc
                ? dt
                : DateTime.SpecifyKind(dt, DateTimeKind.Utc);

        var sec = new DateTime(
            utc.Year,
            utc.Month,
            utc.Day,
            utc.Hour,
            utc.Minute,
            utc.Second,
            DateTimeKind.Utc);

        var frameTicks =
            TimeSpan.TicksPerSecond / (double)selectRate;

        var offsetTicks =
            utc.Ticks - sec.Ticks;

        var rawFrameIndex =
            offsetTicks / frameTicks;

        var frameIdx =
            (int)Math.Round(
                rawFrameIndex,
                MidpointRounding.AwayFromZero);

        if (frameIdx >= selectRate)
        {
            sec = sec.AddSeconds(1);
            frameIdx = 0;
        }

        var nominalTicks =
            sec.Ticks +
            (long)Math.Round(
                frameIdx * frameTicks,
                MidpointRounding.AwayFromZero);

        expectedTs =
            new DateTime(
                nominalTicks,
                DateTimeKind.Utc);

        distanceTicks =
            Math.Abs(utc.Ticks - nominalTicks);

        var toleranceTicks =
            frameTicks * FrameToleranceFraction;

        frameKey = (sec.Ticks, frameIdx);

        return distanceTicks <= toleranceTicks;
    }

    /// <summary>
    /// Obtém a chave de um timestamp que já pertence à grade nominal.
    /// </summary>
    private static (long SecTicks, int FrameIdx) GetExpectedFrameKey(
        DateTime expectedTs,
        int selectRate)
    {
        if (!TryMapTimestampToFrame(
                expectedTs,
                selectRate,
                out var frameKey,
                out _,
                out _))
        {
            // Para timestamps produzidos por EnumerateExpectedFrames isto não deve ocorrer.
            throw new InvalidOperationException(
                "Timestamp esperado não pôde ser associado à própria grade de frames.");
        }

        return frameKey;
    }

    /// <summary>
    /// Enumera os timestamps nominais em [cs, ce), usando ticks para evitar
    /// perda de precisão ou acúmulo de erro por ponto flutuante em AddMilliseconds.
    /// </summary>
    private static IEnumerable<DateTime> EnumerateExpectedFrames(
        DateTime cs,
        DateTime ce,
        int selectRate)
    {
        if (selectRate <= 0)
            throw new ArgumentOutOfRangeException(nameof(selectRate));

        var frameTicks =
            TimeSpan.TicksPerSecond / (double)selectRate;

        var startSec = new DateTime(
            cs.Year,
            cs.Month,
            cs.Day,
            cs.Hour,
            cs.Minute,
            cs.Second,
            DateTimeKind.Utc);

        for (var sec = startSec; sec < ce; sec = sec.AddSeconds(1))
        {
            for (var n = 0; n < selectRate; n++)
            {
                var ticks =
                    sec.Ticks +
                    (long)Math.Round(
                        n * frameTicks,
                        MidpointRounding.AwayFromZero);

                var t = new DateTime(
                    ticks,
                    DateTimeKind.Utc);

                if (t >= cs && t < ce)
                    yield return t;
            }
        }
    }
}