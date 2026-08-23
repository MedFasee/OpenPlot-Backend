using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Features.Runs.Repositories;

public enum PhaseMode
{
    Any,
    Single,
    ABC,
    ThreePhase,
    Deseq,
    SeqPos,
    SeqNeg,
    SeqZero
}

public enum SeqKind { Voltage, Current }
public enum SeqType { Pos, Neg, Zero }

public sealed record SeqRequest(SeqKind Kind, SeqType Seq);
public sealed record UnbalanceRequest(SeqKind Kind);

public sealed record MeasurementsQuery(
    string Quantity,
    string Component,
    PhaseMode PhaseMode = PhaseMode.Any,
    string? Phase = null,
    IReadOnlyList<string>? PmuNames = null,
    string? Unit = null,
    string? ReferenceTerminal = null
);

public sealed record MeasurementRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    DateTime Ts,
    double Value
);

public sealed record PhasorMeasurementRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    string Phase,
    string Component,
    int? VoltLevel,
    DateTime Ts,
    double Value
);

public sealed record PhasorAbcRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    string Phase,
    string Component,
    double? VoltLevel,
    DateTime Ts,
    double Value
);


/// <summary>
/// Frame Wide para cálculo de diferença angular.
/// No modo RAW, corresponde diretamente a uma linha de measurements_wide_2.
/// No modo amostrado, corresponde à linha real escolhida como representante
/// do bucket hierárquico. O Ts é sempre um timestamp real do banco.
/// </summary>
public sealed record AngleFrameRow(
    int PdcPmuId,
    string IdName,
    string PdcName,
    double? VoltLevel,
    DateTime Ts,
    double? AMod,
    double? AAng,
    double? BMod,
    double? BAng,
    double? CMod,
    double? CAng
);

/// <summary>
/// Frame Wide para cálculo de potência.
/// No modo RAW, corresponde diretamente a uma linha de measurements_wide_2.
/// No modo amostrado, corresponde à linha real escolhida como representante
/// de um bucket temporal fixo e hierárquico.
/// </summary>
public sealed record PowerFrameRow(
    int PdcPmuId,
    string IdName,
    string PdcName,
    double? VoltLevel,
    DateTime Ts,
    double? VaMod,
    double? VaAng,
    double? VbMod,
    double? VbAng,
    double? VcMod,
    double? VcAng,
    double? IaMod,
    double? IaAng,
    double? IbMod,
    double? IbAng,
    double? IcMod,
    double? IcAng
);

public interface IMeasurementsRepository
{
    Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null);

    Task<IReadOnlyList<PhasorMeasurementRow>> QueryPhasorAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null);

    Task<IReadOnlyList<PhasorAbcRow>> QueryAbcMagAngAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null);

    Task<IReadOnlyList<AngleFrameRow>> QueryAngleFramesAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null,
        string? phase = null);

    Task<IReadOnlyList<PowerFrameRow>> QueryPowerFramesAsync(
        RunContext ctx,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null);

    Task WarmUpAsync(
        RunContext ctx,
        CancellationToken ct);
}

public sealed class MeasurementsRepository : IMeasurementsRepository
{
    private const int ByRunMeasurementQuality = 29;

    // O preview atual do OpenPlot trabalha com fontes de até 120 fps.
    // Este valor é usado APENAS para decidir quando a janela inteira já cabe
    // em maxPoints e, portanto, deve ser lida RAW sem qualquer agregação.
    // Se futuramente o PDC expuser a taxa nominal no RunContext, substitua
    // esta constante pela taxa real da fonte.
    private const double PreviewMaxExpectedFps = 120.0;

    // INVARIANTE TEMPORAL:
    // todos os níveis de downsampling usam a mesma origem global.
    // O timestamp devolvido pela API NUNCA é o início do bucket: é sempre
    // mw.ts de uma linha realmente existente em measurements_wide_2.
    private static readonly DateTime BucketOriginUtc = DateTime.UnixEpoch;

    // Quantum da hierarquia de seleção. Não é timestamp de saída.
    // Os níveis são 1, 2, 4, 8, 16, 32... ms, todos aninhados.
    private static readonly TimeSpan DefaultMinBucket = TimeSpan.FromMilliseconds(1);

    private readonly IDbConnectionFactory _dbf;
    private readonly ILogger<MeasurementsRepository> _logger;

    public MeasurementsRepository(
        IDbConnectionFactory dbf,
        ILogger<MeasurementsRepository> logger)
    {
        _dbf = dbf ?? throw new ArgumentNullException(nameof(dbf));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    // ============================================================
    // SIMPLE
    // ============================================================
    public async Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null)
    {
        var totalWatch = Stopwatch.StartNew();
        using var db = _dbf.Create();
        var connectionMs = await EnsureConnectionOpenAsync(db, ct);

        var selectedPmus = SelectRunPmus(ctx, q.PmuNames);
        if (selectedPmus.Length == 0)
            return Array.Empty<MeasurementRow>();

        var metadataWatch = Stopwatch.StartNew();
        var signals = await ResolveSignalScopeAsync(
            db,
            ctx.PdcId,
            selectedPmus,
            q.Quantity,
            q.Component,
            q.PhaseMode,
            q.Phase,
            ct);
        metadataWatch.Stop();

        if (signals.Count == 0)
            return Array.Empty<MeasurementRow>();

        var projection = BuildProjection(q.Quantity, q.Component, q.PhaseMode, q.Phase);

        var pdcPmuIds = signals
            .Select(x => x.PdcPmuId)
            .Distinct()
            .OrderBy(x => x)
            .ToArray();

        var sampling = BuildSamplingPlan(
            ctx.FromUtc,
            ctx.ToUtc,
            maxPoints,
            projection.MinimumBucket,
            projection.ForceSampling);

        var rawSql = $@"
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {projection.RawSelectSql}
FROM openplot.measurements_wide_2 mw
WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
  AND mw.ts >= @from_utc
  AND mw.ts <  @to_utc
  AND (mw.quality = @quality OR mw.quality IS NULL)
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sampledSql = $@"
WITH bounds AS (
    SELECT
        time_bucket(
            @bucket_width::interval,
            @from_utc::timestamptz,
            @bucket_origin::timestamptz
        ) AS aligned_from,
        time_bucket(
            @bucket_width::interval,
            @to_utc::timestamptz,
            @bucket_origin::timestamptz
        ) + @bucket_width::interval AS aligned_to
),
representatives AS (
    SELECT
        mw.pdc_pmu_id AS pdc_pmu_id,
        min(mw.ts) AS ts
    FROM openplot.measurements_wide_2 mw
    CROSS JOIN bounds b
    WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
      AND mw.ts >= b.aligned_from
      AND mw.ts <  b.aligned_to
      AND (mw.quality = @quality OR mw.quality IS NULL)
    GROUP BY
        mw.pdc_pmu_id,
        time_bucket(
            @bucket_width::interval,
            mw.ts,
            @bucket_origin::timestamptz
        )
)
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {projection.RawSelectSql}
FROM representatives r
JOIN openplot.measurements_wide_2 mw
  ON mw.pdc_pmu_id = r.pdc_pmu_id
 AND mw.ts = r.ts
WHERE r.ts >= @from_utc
  AND r.ts <  @to_utc
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sql = sampling.UseRaw ? rawSql : sampledSql;

        _logger.LogInformation(
            "[DATA-REQ][QueryAsync][START] pdc={Pdc} quantity={Quantity} component={Component} window=[{From:o}..{To:o}] pmus={Pmus} pdcPmuCount={PdcPmuCount} maxPoints={MaxPoints} sampling={SamplingMode} bucketMs={BucketMs:F3}",
            ctx.PdcName,
            q.Quantity,
            q.Component,
            ctx.FromUtc,
            ctx.ToUtc,
            string.Join(',', selectedPmus),
            pdcPmuIds.Length,
            maxPoints,
            sampling.UseRaw ? "raw" : "hierarchical",
            sampling.UseRaw ? 0d : sampling.BucketWidth.TotalMilliseconds);

        var queryWatch = Stopwatch.StartNew();
        var sampled = (await db.QueryAsync<WideSampleRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_pmu_ids = pdcPmuIds,
                    from_utc = ctx.FromUtc,
                    to_utc = ctx.ToUtc,
                    quality = ByRunMeasurementQuality,
                    bucket_width = sampling.BucketWidth,
                    bucket_origin = BucketOriginUtc
                },
                commandTimeout: 120,
                cancellationToken: ct)))
            .ToList();
        queryWatch.Stop();

        var byPdcPmu = signals
            .GroupBy(x => x.PdcPmuId)
            .ToDictionary(g => g.Key, g => g.ToArray());

        var output = new List<MeasurementRow>();

        foreach (var row in sampled)
        {
            if (!byPdcPmu.TryGetValue(row.PdcPmuId, out var signalRows))
                continue;

            foreach (var signal in signalRows)
            {
                var value = projection.ResolveValue(row, signal.Phase);
                if (!value.HasValue)
                    continue;

                output.Add(new MeasurementRow(
                    signal.SignalId,
                    row.PdcPmuId,
                    signal.IdName,
                    ctx.PdcName,
                    row.Ts,
                    value.Value));
            }
        }

        totalWatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryAsync][END] connectionMs={ConnectionMs} metadataMs={MetadataMs} queryMs={QueryMs} totalMs={TotalMs} sampledFrames={SampledFrames} rows={Rows}",
            connectionMs,
            metadataWatch.ElapsedMilliseconds,
            queryWatch.ElapsedMilliseconds,
            totalWatch.ElapsedMilliseconds,
            sampled.Count,
            output.Count);

        return output;
    }

    // ============================================================
    // PHASOR
    // ============================================================
    public async Task<IReadOnlyList<PhasorMeasurementRow>> QueryPhasorAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null)
    {
        var totalWatch = Stopwatch.StartNew();
        using var db = _dbf.Create();
        var connectionMs = await EnsureConnectionOpenAsync(db, ct);

        var selectedPmus = SelectRunPmus(ctx, q.PmuNames);
        if (selectedPmus.Length == 0)
            return Array.Empty<PhasorMeasurementRow>();

        var metadataWatch = Stopwatch.StartNew();
        var signals = await ResolveSignalScopeAsync(
            db,
            ctx.PdcId,
            selectedPmus,
            q.Quantity,
            q.Component,
            q.PhaseMode,
            q.Phase,
            ct);
        metadataWatch.Stop();

        if (signals.Count == 0)
            return Array.Empty<PhasorMeasurementRow>();

        var projection = BuildProjection(q.Quantity, q.Component, q.PhaseMode, q.Phase);

        var pdcPmuIds = signals
            .Select(x => x.PdcPmuId)
            .Distinct()
            .OrderBy(x => x)
            .ToArray();

        var sampling = BuildSamplingPlan(
            ctx.FromUtc,
            ctx.ToUtc,
            maxPoints,
            projection.MinimumBucket,
            projection.ForceSampling);

        var rawSql = $@"
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {projection.RawSelectSql}
FROM openplot.measurements_wide_2 mw
WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
  AND mw.ts >= @from_utc
  AND mw.ts <  @to_utc
  AND (mw.quality = @quality OR mw.quality IS NULL)
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sampledSql = $@"
WITH bounds AS (
    SELECT
        time_bucket(
            @bucket_width::interval,
            @from_utc::timestamptz,
            @bucket_origin::timestamptz
        ) AS aligned_from,
        time_bucket(
            @bucket_width::interval,
            @to_utc::timestamptz,
            @bucket_origin::timestamptz
        ) + @bucket_width::interval AS aligned_to
),
representatives AS (
    SELECT
        mw.pdc_pmu_id AS pdc_pmu_id,
        min(mw.ts) AS ts
    FROM openplot.measurements_wide_2 mw
    CROSS JOIN bounds b
    WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
      AND mw.ts >= b.aligned_from
      AND mw.ts <  b.aligned_to
      AND (mw.quality = @quality OR mw.quality IS NULL)
    GROUP BY
        mw.pdc_pmu_id,
        time_bucket(
            @bucket_width::interval,
            mw.ts,
            @bucket_origin::timestamptz
        )
)
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {projection.RawSelectSql}
FROM representatives r
JOIN openplot.measurements_wide_2 mw
  ON mw.pdc_pmu_id = r.pdc_pmu_id
 AND mw.ts = r.ts
WHERE r.ts >= @from_utc
  AND r.ts <  @to_utc
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sql = sampling.UseRaw ? rawSql : sampledSql;

        _logger.LogInformation(
            "[DATA-REQ][QueryPhasorAsync][START] pdc={Pdc} quantity={Quantity} component={Component} phase={Phase} window=[{From:o}..{To:o}] pmus={Pmus} pdcPmuCount={PdcPmuCount} maxPoints={MaxPoints} sampling={SamplingMode} bucketMs={BucketMs:F3}",
            ctx.PdcName,
            q.Quantity,
            q.Component,
            q.Phase,
            ctx.FromUtc,
            ctx.ToUtc,
            string.Join(',', selectedPmus),
            pdcPmuIds.Length,
            maxPoints,
            sampling.UseRaw ? "raw" : "hierarchical",
            sampling.UseRaw ? 0d : sampling.BucketWidth.TotalMilliseconds);

        var queryWatch = Stopwatch.StartNew();
        var sampled = (await db.QueryAsync<WideSampleRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_pmu_ids = pdcPmuIds,
                    from_utc = ctx.FromUtc,
                    to_utc = ctx.ToUtc,
                    quality = ByRunMeasurementQuality,
                    bucket_width = sampling.BucketWidth,
                    bucket_origin = BucketOriginUtc
                },
                commandTimeout: 120,
                cancellationToken: ct)))
            .ToList();
        queryWatch.Stop();

        var byPdcPmu = signals
            .GroupBy(x => x.PdcPmuId)
            .ToDictionary(g => g.Key, g => g.ToArray());

        var output = new List<PhasorMeasurementRow>();

        foreach (var row in sampled)
        {
            if (!byPdcPmu.TryGetValue(row.PdcPmuId, out var signalRows))
                continue;

            foreach (var signal in signalRows)
            {
                var value = projection.ResolveValue(row, signal.Phase);
                if (!value.HasValue)
                    continue;

                output.Add(new PhasorMeasurementRow(
                    signal.SignalId,
                    row.PdcPmuId,
                    signal.IdName,
                    ctx.PdcName,
                    signal.Phase,
                    signal.Component,
                    signal.VoltLevel,
                    row.Ts,
                    value.Value));
            }
        }

        totalWatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryPhasorAsync][END] connectionMs={ConnectionMs} metadataMs={MetadataMs} queryMs={QueryMs} totalMs={TotalMs} sampledFrames={SampledFrames} rows={Rows}",
            connectionMs,
            metadataWatch.ElapsedMilliseconds,
            queryWatch.ElapsedMilliseconds,
            totalWatch.ElapsedMilliseconds,
            sampled.Count,
            output.Count);

        return output;
    }

    // ============================================================
    // ABC MAG+ANG
    //
    // O catálogo é resolvido em consulta pequena.
    // A consulta pesada toca SOMENTE measurements_wide_2 por pdc_pmu_id[].
    // A expansão em 6 SignalIds acontece em C# depois da seleção da linha real.
    // ============================================================
    public async Task<IReadOnlyList<PhasorAbcRow>> QueryAbcMagAngAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null)
    {
        var totalWatch = Stopwatch.StartNew();
        using var db = _dbf.Create();
        var connectionMs = await EnsureConnectionOpenAsync(db, ct);

        var k = NormalizeQuantity(kind);
        if (k is not ("voltage" or "current"))
            throw new ArgumentException("kind deve ser 'voltage' ou 'current'.", nameof(kind));

        var effFrom = fromUtc ?? ctx.FromUtc;
        var effTo = toUtc ?? ctx.ToUtc;

        var selectedPmus = SelectRunPmus(ctx, pmuNames);
        if (selectedPmus.Length == 0)
            return Array.Empty<PhasorAbcRow>();

        var metadataWatch = Stopwatch.StartNew();
        var signals = await ResolveAbcSignalScopeAsync(
            db,
            ctx.PdcId,
            selectedPmus,
            k,
            ct);
        metadataWatch.Stop();

        if (signals.Count == 0)
            return Array.Empty<PhasorAbcRow>();

        var pdcPmuIds = signals
            .Select(x => x.PdcPmuId)
            .Distinct()
            .OrderBy(x => x)
            .ToArray();

        var sampling = BuildSamplingPlan(
            effFrom,
            effTo,
            maxPoints,
            DefaultMinBucket);

        var rawColumns = k == "voltage"
            ? @"mw.va_mod_v   AS AMag,
                mw.va_ang_deg AS AAng,
                mw.vb_mod_v   AS BMag,
                mw.vb_ang_deg AS BAng,
                mw.vc_mod_v   AS CMag,
                mw.vc_ang_deg AS CAng"
            : @"mw.ia_mod_a   AS AMag,
                mw.ia_ang_deg AS AAng,
                mw.ib_mod_a   AS BMag,
                mw.ib_ang_deg AS BAng,
                mw.ic_mod_a   AS CMag,
                mw.ic_ang_deg AS CAng";

        var rawSql = $@"
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {rawColumns}
FROM openplot.measurements_wide_2 mw
WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
  AND mw.ts >= @from_utc
  AND mw.ts <  @to_utc
  AND (mw.quality = @quality OR mw.quality IS NULL)
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sampledSql = $@"
WITH bounds AS (
    SELECT
        time_bucket(
            @bucket_width::interval,
            @from_utc::timestamptz,
            @bucket_origin::timestamptz
        ) AS aligned_from,
        time_bucket(
            @bucket_width::interval,
            @to_utc::timestamptz,
            @bucket_origin::timestamptz
        ) + @bucket_width::interval AS aligned_to
),
representatives AS (
    SELECT
        mw.pdc_pmu_id AS pdc_pmu_id,
        min(mw.ts) AS ts
    FROM openplot.measurements_wide_2 mw
    CROSS JOIN bounds b
    WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
      AND mw.ts >= b.aligned_from
      AND mw.ts <  b.aligned_to
      AND (mw.quality = @quality OR mw.quality IS NULL)
    GROUP BY
        mw.pdc_pmu_id,
        time_bucket(
            @bucket_width::interval,
            mw.ts,
            @bucket_origin::timestamptz
        )
)
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {rawColumns}
FROM representatives r
JOIN openplot.measurements_wide_2 mw
  ON mw.pdc_pmu_id = r.pdc_pmu_id
 AND mw.ts = r.ts
WHERE r.ts >= @from_utc
  AND r.ts <  @to_utc
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sql = sampling.UseRaw ? rawSql : sampledSql;

        _logger.LogInformation(
            "[DATA-REQ][QueryAbcMagAngAsync][START] pdc={Pdc} kind={Kind} window=[{From:o}..{To:o}] pmus={Pmus} pdcPmuCount={PdcPmuCount} maxPoints={MaxPoints} sampling={SamplingMode} bucketMs={BucketMs:F3} hotQuery=direct_pdc_pmu_array",
            ctx.PdcName,
            k,
            effFrom,
            effTo,
            string.Join(',', selectedPmus),
            pdcPmuIds.Length,
            maxPoints,
            sampling.UseRaw ? "raw" : "hierarchical",
            sampling.UseRaw ? 0d : sampling.BucketWidth.TotalMilliseconds);

        var queryWatch = Stopwatch.StartNew();
        var sampled = (await db.QueryAsync<AbcWideSampleRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_pmu_ids = pdcPmuIds,
                    from_utc = effFrom,
                    to_utc = effTo,
                    quality = ByRunMeasurementQuality,
                    bucket_width = sampling.BucketWidth,
                    bucket_origin = BucketOriginUtc
                },
                commandTimeout: 120,
                cancellationToken: ct)))
            .ToList();
        queryWatch.Stop();

        var byPdcPmu = signals
            .GroupBy(x => x.PdcPmuId)
            .ToDictionary(g => g.Key, g => g.ToArray());

        var output = new List<PhasorAbcRow>(sampled.Count * 6);

        foreach (var row in sampled)
        {
            if (!byPdcPmu.TryGetValue(row.PdcPmuId, out var signalRows))
                continue;

            foreach (var signal in signalRows)
            {
                var value = ResolveAbcValue(row, signal.Phase, signal.Component);
                if (!value.HasValue)
                    continue;

                output.Add(new PhasorAbcRow(
                    signal.SignalId,
                    row.PdcPmuId,
                    signal.IdName,
                    ctx.PdcName,
                    signal.Phase,
                    signal.Component,
                    signal.VoltLevel,
                    row.Ts,
                    value.Value));
            }
        }

        totalWatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryAbcMagAngAsync][END] connectionMs={ConnectionMs} metadataMs={MetadataMs} queryMs={QueryMs} totalMs={TotalMs} sampledFrames={SampledFrames} rows={Rows}",
            connectionMs,
            metadataWatch.ElapsedMilliseconds,
            queryWatch.ElapsedMilliseconds,
            totalWatch.ElapsedMilliseconds,
            sampled.Count,
            output.Count);

        return output;
    }


    // ============================================================
    // ANGLE DIFF
    //
    // Retorna UMA linha Wide por PMU/frame selecionado.
    //
    // - não expande A/B/C MAG+ANG em 6 PhasorAbcRow;
    // - usa o mesmo SamplingPlan hierárquico das demais consultas;
    // - o timestamp devolvido é SEMPRE mw.ts de uma linha real;
    // - no preview seleciona a linha representante do bucket;
    // - com maxPoints == null lê RAW, preservando a massa integral.
    // ============================================================
    public async Task<IReadOnlyList<AngleFrameRow>> QueryAngleFramesAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null,
        string? phase = null)
    {
        var totalWatch = Stopwatch.StartNew();

        using var db = _dbf.Create();
        var connectionMs = await EnsureConnectionOpenAsync(db, ct);

        var k = NormalizeQuantity(kind);
        if (k is not ("voltage" or "current"))
            throw new ArgumentException(
                "kind deve ser 'voltage' ou 'current'.",
                nameof(kind));

        var normalizedPhase = string.IsNullOrWhiteSpace(phase)
            ? null
            : phase.Trim().ToUpperInvariant();

        if (normalizedPhase is not null &&
            normalizedPhase is not ("A" or "B" or "C"))
        {
            throw new ArgumentException(
                "phase deve ser A, B ou C quando informada.",
                nameof(phase));
        }

        var effFrom = fromUtc ?? ctx.FromUtc;
        var effTo = toUtc ?? ctx.ToUtc;

        var selectedPmus = SelectRunPmus(ctx, pmuNames);
        if (selectedPmus.Length == 0)
            return Array.Empty<AngleFrameRow>();

        var metadataWatch = Stopwatch.StartNew();

        var pmus = await ResolvePmuScopeAsync(
            db,
            ctx.PdcId,
            selectedPmus,
            ct);

        metadataWatch.Stop();

        if (pmus.Count == 0)
            return Array.Empty<AngleFrameRow>();

        var pdcPmuIds = pmus
            .Select(x => x.PdcPmuId)
            .Distinct()
            .OrderBy(x => x)
            .ToArray();

        var sampling = BuildSamplingPlan(
            effFrom,
            effTo,
            maxPoints,
            DefaultMinBucket);

        var rawColumns = BuildAngleFrameColumns(k, normalizedPhase);

        var rawSql = $@"
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {rawColumns}
FROM openplot.measurements_wide_2 mw
WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
  AND mw.ts >= @from_utc
  AND mw.ts <  @to_utc
  AND (mw.quality = @quality OR mw.quality IS NULL)
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sampledSql = $@"
WITH bounds AS (
    SELECT
        time_bucket(
            @bucket_width::interval,
            @from_utc::timestamptz,
            @bucket_origin::timestamptz
        ) AS aligned_from,
        time_bucket(
            @bucket_width::interval,
            @to_utc::timestamptz,
            @bucket_origin::timestamptz
        ) + @bucket_width::interval AS aligned_to
),
representatives AS (
    SELECT
        mw.pdc_pmu_id AS pdc_pmu_id,
        min(mw.ts) AS ts
    FROM openplot.measurements_wide_2 mw
    CROSS JOIN bounds b
    WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
      AND mw.ts >= b.aligned_from
      AND mw.ts <  b.aligned_to
      AND (mw.quality = @quality OR mw.quality IS NULL)
    GROUP BY
        mw.pdc_pmu_id,
        time_bucket(
            @bucket_width::interval,
            mw.ts,
            @bucket_origin::timestamptz
        )
)
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,
    {rawColumns}
FROM representatives r
JOIN openplot.measurements_wide_2 mw
  ON mw.pdc_pmu_id = r.pdc_pmu_id
 AND mw.ts = r.ts
WHERE r.ts >= @from_utc
  AND r.ts <  @to_utc
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sql = sampling.UseRaw
            ? rawSql
            : sampledSql;

        _logger.LogInformation(
            "[DATA-REQ][QueryAngleFramesAsync][START] pdc={Pdc} kind={Kind} phase={Phase} window=[{From:o}..{To:o}] pmus={Pmus} pdcPmuCount={PdcPmuCount} maxPoints={MaxPoints} sampling={SamplingMode} bucketMs={BucketMs:F3} hotQuery=angle_wide_frame",
            ctx.PdcName,
            k,
            normalizedPhase ?? "SEQ",
            effFrom,
            effTo,
            string.Join(',', selectedPmus),
            pdcPmuIds.Length,
            maxPoints,
            sampling.UseRaw ? "raw" : "hierarchical",
            sampling.UseRaw ? 0d : sampling.BucketWidth.TotalMilliseconds);

        var queryWatch = Stopwatch.StartNew();

        var sampled = (await db.QueryAsync<AngleWideSampleRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_pmu_ids = pdcPmuIds,
                    from_utc = effFrom,
                    to_utc = effTo,
                    quality = ByRunMeasurementQuality,
                    bucket_width = sampling.BucketWidth,
                    bucket_origin = BucketOriginUtc
                },
                commandTimeout: 120,
                cancellationToken: ct)))
            .ToList();

        queryWatch.Stop();

        var pmuMap = pmus.ToDictionary(x => x.PdcPmuId);

        var output = new List<AngleFrameRow>(sampled.Count);

        foreach (var row in sampled)
        {
            if (!pmuMap.TryGetValue(row.PdcPmuId, out var pmu))
                continue;

            output.Add(new AngleFrameRow(
                row.PdcPmuId,
                pmu.IdName,
                ctx.PdcName,
                pmu.VoltLevel,
                row.Ts,
                row.AMod,
                row.AAng,
                row.BMod,
                row.BAng,
                row.CMod,
                row.CAng));
        }

        totalWatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryAngleFramesAsync][END] connectionMs={ConnectionMs} metadataMs={MetadataMs} queryMs={QueryMs} totalMs={TotalMs} frames={Frames}",
            connectionMs,
            metadataWatch.ElapsedMilliseconds,
            queryWatch.ElapsedMilliseconds,
            totalWatch.ElapsedMilliseconds,
            output.Count);

        return output;
    }

    // ============================================================
    // POWER
    //
    // V e I são lidos juntos em UMA única passagem pela Wide.
    // Não usa signal na consulta pesada.
    // ============================================================
    public async Task<IReadOnlyList<PowerFrameRow>> QueryPowerFramesAsync(
        RunContext ctx,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null)
    {
        var totalWatch = Stopwatch.StartNew();
        using var db = _dbf.Create();
        var connectionMs = await EnsureConnectionOpenAsync(db, ct);

        var effFrom = fromUtc ?? ctx.FromUtc;
        var effTo = toUtc ?? ctx.ToUtc;

        var selectedPmus = SelectRunPmus(ctx, pmuNames);
        if (selectedPmus.Length == 0)
            return Array.Empty<PowerFrameRow>();

        var metadataWatch = Stopwatch.StartNew();
        var pmus = await ResolvePmuScopeAsync(
            db,
            ctx.PdcId,
            selectedPmus,
            ct);
        metadataWatch.Stop();

        if (pmus.Count == 0)
            return Array.Empty<PowerFrameRow>();

        var pdcPmuIds = pmus
            .Select(x => x.PdcPmuId)
            .Distinct()
            .OrderBy(x => x)
            .ToArray();

        var sampling = BuildSamplingPlan(
            effFrom,
            effTo,
            maxPoints,
            DefaultMinBucket);

        const string rawSql = @"
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,

    mw.va_mod_v   AS VaMod,
    mw.va_ang_deg AS VaAng,
    mw.vb_mod_v   AS VbMod,
    mw.vb_ang_deg AS VbAng,
    mw.vc_mod_v   AS VcMod,
    mw.vc_ang_deg AS VcAng,

    mw.ia_mod_a   AS IaMod,
    mw.ia_ang_deg AS IaAng,
    mw.ib_mod_a   AS IbMod,
    mw.ib_ang_deg AS IbAng,
    mw.ic_mod_a   AS IcMod,
    mw.ic_ang_deg AS IcAng

FROM openplot.measurements_wide_2 mw
WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
  AND mw.ts >= @from_utc
  AND mw.ts <  @to_utc
  AND (mw.quality = @quality OR mw.quality IS NULL)
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        const string sampledSql = @"
WITH bounds AS (
    SELECT
        time_bucket(
            @bucket_width::interval,
            @from_utc::timestamptz,
            @bucket_origin::timestamptz
        ) AS aligned_from,
        time_bucket(
            @bucket_width::interval,
            @to_utc::timestamptz,
            @bucket_origin::timestamptz
        ) + @bucket_width::interval AS aligned_to
),
representatives AS (
    SELECT
        mw.pdc_pmu_id AS pdc_pmu_id,
        min(mw.ts) AS ts
    FROM openplot.measurements_wide_2 mw
    CROSS JOIN bounds b
    WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
      AND mw.ts >= b.aligned_from
      AND mw.ts <  b.aligned_to
      AND (mw.quality = @quality OR mw.quality IS NULL)
    GROUP BY
        mw.pdc_pmu_id,
        time_bucket(
            @bucket_width::interval,
            mw.ts,
            @bucket_origin::timestamptz
        )
)
SELECT
    mw.pdc_pmu_id AS PdcPmuId,
    mw.ts AS Ts,

    mw.va_mod_v   AS VaMod,
    mw.va_ang_deg AS VaAng,
    mw.vb_mod_v   AS VbMod,
    mw.vb_ang_deg AS VbAng,
    mw.vc_mod_v   AS VcMod,
    mw.vc_ang_deg AS VcAng,

    mw.ia_mod_a   AS IaMod,
    mw.ia_ang_deg AS IaAng,
    mw.ib_mod_a   AS IbMod,
    mw.ib_ang_deg AS IbAng,
    mw.ic_mod_a   AS IcMod,
    mw.ic_ang_deg AS IcAng

FROM representatives r
JOIN openplot.measurements_wide_2 mw
  ON mw.pdc_pmu_id = r.pdc_pmu_id
 AND mw.ts = r.ts
WHERE r.ts >= @from_utc
  AND r.ts <  @to_utc
ORDER BY
    mw.pdc_pmu_id,
    mw.ts;";

        var sql = sampling.UseRaw ? rawSql : sampledSql;

        _logger.LogInformation(
            "[DATA-REQ][QueryPowerFramesAsync][START] pdc={Pdc} window=[{From:o}..{To:o}] pmus={Pmus} pdcPmuCount={PdcPmuCount} maxPoints={MaxPoints} sampling={SamplingMode} bucketMs={BucketMs:F3} hotQuery=one_scan_vi",
            ctx.PdcName,
            effFrom,
            effTo,
            string.Join(',', selectedPmus),
            pdcPmuIds.Length,
            maxPoints,
            sampling.UseRaw ? "raw" : "hierarchical",
            sampling.UseRaw ? 0d : sampling.BucketWidth.TotalMilliseconds);

        var queryWatch = Stopwatch.StartNew();
        var sampled = (await db.QueryAsync<PowerWideSampleRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_pmu_ids = pdcPmuIds,
                    from_utc = effFrom,
                    to_utc = effTo,
                    quality = ByRunMeasurementQuality,
                    bucket_width = sampling.BucketWidth,
                    bucket_origin = BucketOriginUtc
                },
                commandTimeout: 120,
                cancellationToken: ct)))
            .ToList();
        queryWatch.Stop();

        var pmuMap = pmus.ToDictionary(x => x.PdcPmuId);

        var output = sampled
            .Where(x => pmuMap.ContainsKey(x.PdcPmuId))
            .Select(x =>
            {
                var pmu = pmuMap[x.PdcPmuId];

                return new PowerFrameRow(
                    x.PdcPmuId,
                    pmu.IdName,
                    ctx.PdcName,
                    pmu.VoltLevel,
                    x.Ts,
                    x.VaMod,
                    x.VaAng,
                    x.VbMod,
                    x.VbAng,
                    x.VcMod,
                    x.VcAng,
                    x.IaMod,
                    x.IaAng,
                    x.IbMod,
                    x.IbAng,
                    x.IcMod,
                    x.IcAng);
            })
            .ToList();

        totalWatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryPowerFramesAsync][END] connectionMs={ConnectionMs} metadataMs={MetadataMs} queryMs={QueryMs} totalMs={TotalMs} frames={Frames}",
            connectionMs,
            metadataWatch.ElapsedMilliseconds,
            queryWatch.ElapsedMilliseconds,
            totalWatch.ElapsedMilliseconds,
            output.Count);

        return output;
    }

    // ============================================================
    // WARM-UP
    // ============================================================
    public async Task WarmUpAsync(
        RunContext ctx,
        CancellationToken ct)
    {
        using var db = _dbf.Create();
        await EnsureConnectionOpenAsync(db, ct);

        var pmuScope = await ResolvePmuScopeAsync(db, ctx.PdcId, ctx.PmuNames, ct);
        if (pmuScope.Count == 0)
            return;

        var pdcPmuIds = pmuScope.Select(r => r.PdcPmuId).ToArray();

        const string sql = @"
SELECT 1
FROM openplot.measurements_wide_2 mw
WHERE mw.pdc_pmu_id = ANY(@pdc_pmu_ids)
  AND mw.ts >= @from_utc
  AND mw.ts <  @to_utc
LIMIT 1";

        await db.ExecuteScalarAsync<int?>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_pmu_ids = pdcPmuIds,
                    from_utc    = ctx.FromUtc,
                    to_utc      = ctx.ToUtc
                },
                commandTimeout: 60,
                cancellationToken: ct));

        _logger.LogDebug(
            "[WARM-UP] measurements_wide_2 aquecida: pdc={Pdc} pmuCount={Count} window=[{From:o}..{To:o}]",
            ctx.PdcName, pdcPmuIds.Length, ctx.FromUtc, ctx.ToUtc);
    }

    // ============================================================
    // METADATA RESOLUTION
    // ============================================================
    private async Task<List<PmuScopeRow>> ResolvePmuScopeAsync(
        IDbConnection db,
        int pdcId,
        IReadOnlyList<string> pmuNames,
        CancellationToken ct)
    {
        const string sql = @"
SELECT
    pp.pdc_pmu_id AS PdcPmuId,
    p.id_name     AS IdName,
    p.volt_level  AS VoltLevel
FROM openplot.pdc_pmu pp
JOIN openplot.pmu p
  ON p.pmu_id = pp.pmu_id
WHERE pp.pdc_id = @pdc_id
  AND p.id_name = ANY(@pmu_names)
ORDER BY pp.pdc_pmu_id;";

        return (await db.QueryAsync<PmuScopeRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_id = pdcId,
                    pmu_names = pmuNames.ToArray()
                },
                commandTimeout: 30,
                cancellationToken: ct)))
            .ToList();
    }

    private async Task<List<SignalScopeRow>> ResolveSignalScopeAsync(
        IDbConnection db,
        int pdcId,
        IReadOnlyList<string> pmuNames,
        string quantity,
        string component,
        PhaseMode phaseMode,
        string? phase,
        CancellationToken ct)
    {
        var normalizedQuantity = NormalizeQuantity(quantity);
        var normalizedComponent = (component ?? string.Empty).Trim().ToLowerInvariant();
        var phaseModeName = phaseMode switch
        {
            PhaseMode.Single => "single",
            PhaseMode.ABC or PhaseMode.ThreePhase => "abc",
            _ => "any"
        };

        const string sql = @"
SELECT
    s.signal_id                    AS SignalId,
    s.pdc_pmu_id                   AS PdcPmuId,
    p.id_name                      AS IdName,
    UPPER(COALESCE(s.phase::text,''))     AS Phase,
    UPPER(COALESCE(s.component::text,'')) AS Component,
    p.volt_level                   AS VoltLevel
FROM openplot.pdc_pmu pp
JOIN openplot.pmu p
  ON p.pmu_id = pp.pmu_id
JOIN openplot.signal s
  ON s.pdc_pmu_id = pp.pdc_pmu_id
WHERE pp.pdc_id = @pdc_id
  AND p.id_name = ANY(@pmu_names)
  AND (
       (@quantity = 'voltage'   AND LOWER(s.quantity::text) IN ('voltage','v'))
    OR (@quantity = 'current'   AND LOWER(s.quantity::text) IN ('current','i'))
    OR (@quantity = 'frequency' AND LOWER(s.quantity::text) IN ('frequency','freq'))
    OR (@quantity = 'digital'   AND LOWER(s.quantity::text) IN ('digital','d'))
    OR (LOWER(s.quantity::text) = @quantity)
  )
  AND LOWER(s.component::text) = @component
  AND (
       @quantity <> 'digital'
       OR UPPER(COALESCE(s.name, '')) = 'cfds_dig'
  )
  AND (
       @phase_mode = 'any'
    OR (@phase_mode = 'abc'    AND UPPER(s.phase::text) IN ('A','B','C'))
    OR (@phase_mode = 'single' AND UPPER(s.phase::text) = UPPER(@phase))
  )
ORDER BY
    s.pdc_pmu_id,
    s.signal_id;";

        return (await db.QueryAsync<SignalScopeRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_id = pdcId,
                    pmu_names = pmuNames.ToArray(),
                    quantity = normalizedQuantity,
                    component = normalizedComponent,
                    phase_mode = phaseModeName,
                    phase
                },
                commandTimeout: 30,
                cancellationToken: ct)))
            .ToList();
    }

    private async Task<List<SignalScopeRow>> ResolveAbcSignalScopeAsync(
        IDbConnection db,
        int pdcId,
        IReadOnlyList<string> pmuNames,
        string kind,
        CancellationToken ct)
    {
        const string sql = @"
SELECT
    s.signal_id                    AS SignalId,
    s.pdc_pmu_id                   AS PdcPmuId,
    p.id_name                      AS IdName,
    UPPER(s.phase::text)           AS Phase,
    UPPER(s.component::text)       AS Component,
    p.volt_level                   AS VoltLevel
FROM openplot.pdc_pmu pp
JOIN openplot.pmu p
  ON p.pmu_id = pp.pmu_id
JOIN openplot.signal s
  ON s.pdc_pmu_id = pp.pdc_pmu_id
WHERE pp.pdc_id = @pdc_id
  AND p.id_name = ANY(@pmu_names)
  AND (
       (@kind = 'voltage' AND LOWER(s.quantity::text) IN ('voltage','v'))
    OR (@kind = 'current' AND LOWER(s.quantity::text) IN ('current','i'))
  )
  AND UPPER(s.phase::text) IN ('A','B','C')
  AND UPPER(s.component::text) IN ('MAG','ANG')
ORDER BY
    s.pdc_pmu_id,
    s.signal_id;";

        return (await db.QueryAsync<SignalScopeRow>(
            new CommandDefinition(
                sql,
                new
                {
                    pdc_id = pdcId,
                    pmu_names = pmuNames.ToArray(),
                    kind
                },
                commandTimeout: 30,
                cancellationToken: ct)))
            .ToList();
    }

    // ============================================================
    // HELPERS
    // ============================================================
    private async Task<long> EnsureConnectionOpenAsync(
        IDbConnection db,
        CancellationToken ct)
    {
        var watch = Stopwatch.StartNew();

        if (db.State != ConnectionState.Open)
        {
            if (db is DbConnection dbc)
                await dbc.OpenAsync(ct);
            else
                db.Open();
        }

        watch.Stop();
        return watch.ElapsedMilliseconds;
    }

    private static string[] SelectRunPmus(
        RunContext ctx,
        IReadOnlyList<string>? requested)
    {
        if (requested is null || requested.Count == 0)
            return ctx.PmuNames.ToArray();

        var requestedSet = new HashSet<string>(
            requested.Where(x => !string.IsNullOrWhiteSpace(x)),
            StringComparer.OrdinalIgnoreCase);

        return ctx.PmuNames
            .Where(requestedSet.Contains)
            .ToArray();
    }

    private readonly record struct SamplingPlan(
        bool UseRaw,
        TimeSpan BucketWidth);

    private static SamplingPlan BuildSamplingPlan(
        DateTime fromUtc,
        DateTime toUtc,
        int? maxPoints,
        TimeSpan minimumBucket,
        bool forceSampling = false)
    {
        if (toUtc <= fromUtc)
            throw new ArgumentException("A janela deve satisfazer fromUtc < toUtc.");

        if (minimumBucket <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                nameof(minimumBucket),
                "minimumBucket deve ser maior que zero.");

        // Sem limite de preview, normalmente o consumidor pede fidelidade
        // integral. FREQ/DFREQ são a exceção: são produtos a 1 fps e, por
        // isso, continuam amostrados mesmo quando maxPoints=null.
        if (maxPoints is null || maxPoints <= 0)
        {
            return forceSampling
                ? new SamplingPlan(
                    UseRaw: false,
                    BucketWidth: minimumBucket)
                : new SamplingPlan(
                    UseRaw: true,
                    BucketWidth: minimumBucket);
        }

        // Para as fontes atuais de até 120 fps, se toda a janela já cabe no
        // orçamento, a consulta RAW é simultaneamente mais fiel e mais barata
        // que GROUP BY + time_bucket. Produtos com forceSampling nunca usam
        // este atalho.
        var expectedRawPointsPerPmu =
            (toUtc - fromUtc).TotalSeconds * PreviewMaxExpectedFps;

        if (!forceSampling &&
            expectedRawPointsPerPmu <= maxPoints.Value)
        {
            return new SamplingPlan(
                UseRaw: true,
                BucketWidth: minimumBucket);
        }

        return new SamplingPlan(
            UseRaw: false,
            BucketWidth: ComputeHierarchicalBucketWidth(
                fromUtc,
                toUtc,
                maxPoints.Value,
                minimumBucket));
    }

    private static TimeSpan ComputeHierarchicalBucketWidth(
        DateTime fromUtc,
        DateTime toUtc,
        int maxPoints,
        TimeSpan minimumBucket)
    {
        if (maxPoints <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPoints));

        // Reservamos uma posição para o efeito de borda da grade global.
        // Isso evita ultrapassar o orçamento apenas porque a janela começa
        // no meio de um bucket.
        var targetBuckets = Math.Max(1, maxPoints - 1);

        var requiredTicks = Math.Max(
            minimumBucket.Ticks,
            (long)Math.Ceiling(
                (toUtc - fromUtc).Ticks / (double)targetBuckets));

        // Hierarquia estritamente aninhada: minimumBucket * 2^N.
        // Como todos os níveis usam BucketOriginUtc, um representante de um
        // nível grosso é também representante de algum bucket do nível fino.
        // Logo, o zoom só revela/remove registros; não recompõe timestamps.
        var bucketTicks = minimumBucket.Ticks;

        while (bucketTicks < requiredTicks)
        {
            if (bucketTicks > TimeSpan.MaxValue.Ticks / 2)
                throw new OverflowException("Bucket temporal excedeu TimeSpan.MaxValue.");

            bucketTicks *= 2;
        }

        return TimeSpan.FromTicks(bucketTicks);
    }

    private static string NormalizeQuantity(string quantity)
    {
        var q = (quantity ?? string.Empty).Trim().ToLowerInvariant();

        return q switch
        {
            "v" => "voltage",
            "i" => "current",
            "freq" => "frequency",
            "d" => "digital",
            _ => q
        };
    }

    private sealed class PmuScopeRow
    {
        public int PdcPmuId { get; set; }
        public string IdName { get; set; } = string.Empty;
        public double? VoltLevel { get; set; }
    }

    private sealed class SignalScopeRow
    {
        public int SignalId { get; set; }
        public int PdcPmuId { get; set; }
        public string IdName { get; set; } = string.Empty;
        public string Phase { get; set; } = string.Empty;
        public string Component { get; set; } = string.Empty;
        public int? VoltLevel { get; set; }
    }

    private sealed record WideProjection(
        string RawSelectSql,
        TimeSpan MinimumBucket,
        bool ForceSampling,
        Func<WideSampleRow, string, double?> ResolveValue);

    private static WideProjection BuildProjection(
        string quantity,
        string component,
        PhaseMode phaseMode,
        string? phase)
    {
        var q = NormalizeQuantity(quantity);
        var c = (component ?? string.Empty).Trim().ToUpperInvariant();

        if (q == "frequency" && c == "FREQ")
        {
            return new WideProjection(
                "mw.frequency_hz AS ValueAny",
                DefaultMinBucket,
                true,
                (row, _) => row.ValueAny);
        }

        if (q == "frequency" && c == "DFREQ")
        {
            return new WideProjection(
                "mw.delta_freq_hz AS ValueAny",
                DefaultMinBucket,
                true,
                (row, _) => row.ValueAny);
        }

        if (q == "digital" && c == "DIG")
        {
            return new WideProjection(
                "mw.cfds_dig AS ValueAny",
                DefaultMinBucket,
                false,
                (row, _) => row.ValueAny);
        }

        if (q == "voltage")
        {
            return c switch
            {
                "MAG" => PhaseProjection(
                    "va_mod_v", "vb_mod_v", "vc_mod_v",
                    phaseMode, phase),

                "ANG" => PhaseProjection(
                    "va_ang_deg", "vb_ang_deg", "vc_ang_deg",
                    phaseMode, phase),

                "THD" => PhaseProjection(
                    "vthd_a_pct", "vthd_b_pct", "vthd_c_pct",
                    phaseMode, phase),

                _ => throw new NotSupportedException(
                    $"Componente de tensão não suportado na Wide: '{component}'.")
            };
        }

        if (q == "current")
        {
            return c switch
            {
                "MAG" => PhaseProjection(
                    "ia_mod_a", "ib_mod_a", "ic_mod_a",
                    phaseMode, phase),

                "ANG" => PhaseProjection(
                    "ia_ang_deg", "ib_ang_deg", "ic_ang_deg",
                    phaseMode, phase),

                "THD" => PhaseProjection(
                    "cthd_a_pct", "cthd_b_pct", "cthd_c_pct",
                    phaseMode, phase),

                _ => throw new NotSupportedException(
                    $"Componente de corrente não suportado na Wide: '{component}'.")
            };
        }

        throw new NotSupportedException(
            $"Mapeamento Wide não suportado: quantity='{quantity}', component='{component}'.");
    }

    private static WideProjection PhaseProjection(
        string colA,
        string colB,
        string colC,
        PhaseMode phaseMode,
        string? phase)
    {
        // Em PhaseMode.Single a hypertable columnar deve descomprimir somente
        // a coluna física realmente pedida.
        if (phaseMode == PhaseMode.Single)
        {
            var p = (phase ?? string.Empty).Trim().ToUpperInvariant();

            var selectedColumn = p switch
            {
                "A" => colA,
                "B" => colB,
                "C" => colC,
                _ => throw new ArgumentException(
                    "PhaseMode.Single exige phase=A, B ou C.",
                    nameof(phase))
            };

            return new WideProjection(
                $"mw.{selectedColumn} AS ValueAny",
                DefaultMinBucket,
                false,
                (row, _) => row.ValueAny);
        }

        return new WideProjection(
            $@"mw.{colA} AS ValueA,
                mw.{colB} AS ValueB,
                mw.{colC} AS ValueC",
            DefaultMinBucket,
            false,
            (row, currentPhase) => currentPhase.ToUpperInvariant() switch
            {
                "A" => row.ValueA,
                "B" => row.ValueB,
                "C" => row.ValueC,
                _ => null
            });
    }

    private static string BuildAngleFrameColumns(
        string kind,
        string? normalizedPhase)
    {
        if (normalizedPhase is not null)
        {
            return (kind, normalizedPhase) switch
            {
                ("voltage", "A") => "mw.va_ang_deg AS AAng",
                ("voltage", "B") => "mw.vb_ang_deg AS BAng",
                ("voltage", "C") => "mw.vc_ang_deg AS CAng",
                ("current", "A") => "mw.ia_ang_deg AS AAng",
                ("current", "B") => "mw.ib_ang_deg AS BAng",
                ("current", "C") => "mw.ic_ang_deg AS CAng",
                _ => throw new NotSupportedException(
                    $"AngleDiff não suportado: kind='{kind}', phase='{normalizedPhase}'.")
            };
        }

        return kind == "voltage"
            ? @"mw.va_mod_v   AS AMod,
                mw.va_ang_deg AS AAng,
                mw.vb_mod_v   AS BMod,
                mw.vb_ang_deg AS BAng,
                mw.vc_mod_v   AS CMod,
                mw.vc_ang_deg AS CAng"
            : @"mw.ia_mod_a   AS AMod,
                mw.ia_ang_deg AS AAng,
                mw.ib_mod_a   AS BMod,
                mw.ib_ang_deg AS BAng,
                mw.ic_mod_a   AS CMod,
                mw.ic_ang_deg AS CAng";
    }

    private static double? ResolveAbcValue(
        AbcWideSampleRow row,
        string phase,
        string component)
    {
        return (phase.ToUpperInvariant(), component.ToUpperInvariant()) switch
        {
            ("A", "MAG") => row.AMag,
            ("A", "ANG") => row.AAng,
            ("B", "MAG") => row.BMag,
            ("B", "ANG") => row.BAng,
            ("C", "MAG") => row.CMag,
            ("C", "ANG") => row.CAng,
            _ => null
        };
    }

    private sealed class WideSampleRow
    {
        public int PdcPmuId { get; set; }
        public DateTime Ts { get; set; }
        public double? ValueAny { get; set; }
        public double? ValueA { get; set; }
        public double? ValueB { get; set; }
        public double? ValueC { get; set; }
    }

    private sealed class AbcWideSampleRow
    {
        public int PdcPmuId { get; set; }
        public DateTime Ts { get; set; }
        public double? AMag { get; set; }
        public double? AAng { get; set; }
        public double? BMag { get; set; }
        public double? BAng { get; set; }
        public double? CMag { get; set; }
        public double? CAng { get; set; }
    }


    private sealed class AngleWideSampleRow
    {
        public int PdcPmuId { get; set; }
        public DateTime Ts { get; set; }
        public double? AMod { get; set; }
        public double? AAng { get; set; }
        public double? BMod { get; set; }
        public double? BAng { get; set; }
        public double? CMod { get; set; }
        public double? CAng { get; set; }
    }

    private sealed class PowerWideSampleRow
    {
        public int PdcPmuId { get; set; }
        public DateTime Ts { get; set; }
        public double? VaMod { get; set; }
        public double? VaAng { get; set; }
        public double? VbMod { get; set; }
        public double? VbAng { get; set; }
        public double? VcMod { get; set; }
        public double? VcAng { get; set; }
        public double? IaMod { get; set; }
        public double? IaAng { get; set; }
        public double? IbMod { get; set; }
        public double? IbAng { get; set; }
        public double? IcMod { get; set; }
        public double? IcAng { get; set; }
    }
}