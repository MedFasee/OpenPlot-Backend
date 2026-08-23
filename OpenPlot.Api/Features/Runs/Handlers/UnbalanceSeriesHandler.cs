using System.Diagnostics;
using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class UnbalanceSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down =
        new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IUiMenuService _uiMenus;
    private readonly ILogger<UnbalanceSeriesHandler> _logger;

    public UnbalanceSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepo,
        IUiMenuService uiMenus,
        ILogger<UnbalanceSeriesHandler> logger)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _seriesAssembly = seriesAssembly;
        _cacheRepo = cacheRepo;
        _uiMenus = uiMenus;
        _logger = logger;
    }

    public async Task<IResult> HandleAsync(
        UnbalanceRunQuery q,
        UnbalanceRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var ctx = await _runs.ResolveAsync(
            q.RunId,
            w.FromUtc,
            w.ToUtc,
            ct);

        if (ctx is null)
            return Results.NotFound(
                "run_id não encontrado.");

        var kind = req.Kind == SeqKind.Current
            ? "current"
            : "voltage";

        _logger.LogInformation(
            "[BYRUN][UNBALANCE][FRONT][START] runId={RunId} kind={Kind} maxPoints={MaxPoints}",
            q.RunId,
            kind,
            noDownsample ? "all" : maxPts);

        var frontWatch = Stopwatch.StartNew();

        // Wide nativo: um frame já contém A/B/C MAG+ANG.
        var rows = await _meas.QueryAngleFramesAsync(
            ctx,
            kind,
            pmuList.Count == 0
                ? null
                : pmuList,
            w.FromUtc,
            w.ToUtc,
            ct,
            noDownsample ? null : maxPts,
            phase: null);

        frontWatch.Stop();

        _logger.LogInformation(
            "[BYRUN][UNBALANCE][FRONT][END] runId={RunId} kind={Kind} elapsedMs={ElapsedMs} frames={Frames}",
            q.RunId,
            kind,
            frontWatch.ElapsedMilliseconds,
            rows.Count);

        if (rows.Count == 0)
            return Results.NotFound(
                "Nenhuma PMU encontrada para este run/kind.");

        var projection = BuildUnbalanceProjection(
            rows,
            kind,
            noDownsample,
            maxPts);

        var series = projection.Series;
        var windowFrom =
            w.FromUtc ?? projection.WindowFrom;
        var windowTo =
            w.ToUtc ?? projection.WindowTo;

        var cacheId = Guid.NewGuid();

        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;

            _logger.LogInformation(
                "[BYRUN][UNBALANCE][CACHE-BG][START] runId={RunId} kind={Kind} cacheId={CacheId}",
                q.RunId,
                kind,
                cacheId);

            try
            {
                var fullRows =
                    await _meas.QueryAngleFramesAsync(
                        ctx,
                        kind,
                        pmuList.Count == 0
                            ? null
                            : pmuList,
                        w.FromUtc,
                        w.ToUtc,
                        CancellationToken.None,
                        maxPoints: null,
                        phase: null);

                fullRowsCount = fullRows.Count;

                if (fullRowsCount == 0)
                    return;

                var fullProjection =
                    BuildUnbalanceProjection(
                        fullRows,
                        kind,
                        true,
                        int.MaxValue);

                var cacheSeriesFull =
                    fullProjection.CachePoints
                        .GroupBy(x => x.pmuId)
                        .Select(g =>
                            _seriesAssembly.BuildCacheSeries(
                                signalId: 0,
                                pdcPmuId: 0,
                                idName: g.Key,
                                pdcName: ctx.PdcName,
                                referenceTerminal: null,
                                unit: "%",
                                phase: null,
                                quantity: kind,
                                component: "ratio",
                                points: g.Select(
                                    x => (x.ts, x.value))))
                        .ToList();

                var cachePayloadFull =
                    _seriesAssembly.BuildCachePayload(
                        w.FromUtc ??
                        fullProjection.WindowFrom,
                        w.ToUtc ??
                        fullProjection.WindowTo,
                        ctx.SelectRate ?? 0,
                        cacheSeriesFull);

                await _cacheRepo.SaveAsync(
                    cacheId,
                    q.RunId,
                    cachePayloadFull,
                    CancellationToken.None);

                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Falha ao persistir cache assíncrono de unbalance/by-run. runId={RunId}",
                    q.RunId);
            }
            finally
            {
                bgWatch.Stop();

                _logger.LogInformation(
                    "[BYRUN][UNBALANCE][CACHE-BG][END] runId={RunId} kind={Kind} cacheId={CacheId} elapsedMs={ElapsedMs} frames={Frames} persisted={Persisted}",
                    q.RunId,
                    kind,
                    cacheId,
                    bgWatch.ElapsedMilliseconds,
                    fullRowsCount,
                    persisted);
            }
        });

        var meas = new MeasurementsQuery(
            Quantity: kind,
            Component: "ratio",
            Unit: "%");

        var plotMeta = _meta.Build(
            w,
            ctx,
            meas);

        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(
                WindowFromUtc: windowFrom,
                WindowToUtc: windowTo,
                SelectRate: ctx.SelectRate,
                TotalSeriesCount: series.Count,
                ValidSeriesCount: series.Count,
                Quantity: kind,
                Component: "ratio"));

        var response =
            SeriesResponseBuilderExtensions
                .BuildSeriesResponse(
                    q.RunId,
                    windowFrom,
                    windowTo,
                    series,
                    plotMeta)
                .WithModes(resolvedModes)
                .WithCacheId(cacheId)
                .WithResolved(
                    ctx.PdcName,
                    series.Count)
                .WithTypeFields(
                    new Dictionary<string, object?>
                    {
                        ["unit"] = "percent",
                        ["kind"] = kind,
                        ["metric"] = "unbalance"
                    })
                .Build();

        return Results.Ok(response);
    }

    /// <summary>
    /// Mantém exatamente as rotinas de sequência existentes.
    /// A otimização ocorre antes do cálculo: cada timestamp chega em um
    /// AngleFrameRow, em vez de seis PhasorAbcRow.
    /// </summary>
    private UnbalanceProjection BuildUnbalanceProjection(
        IReadOnlyList<AngleFrameRow> rows,
        string kind,
        bool noDownsample,
        int maxPts)
    {
        static List<(DateTime ts, double ratio)>
            RatioPointwise(
                List<(DateTime ts, double mag)> neg,
                List<(DateTime ts, double mag)> pos,
                TimeSpan tolerance)
        {
            var output =
                new List<(DateTime ts, double ratio)>();

            var i = 0;
            var j = 0;

            while (i < neg.Count &&
                   j < pos.Count)
            {
                var tn = neg[i].ts;
                var tp = pos[j].ts;
                var t = tn > tp
                    ? tn
                    : tp;

                while (i < neg.Count &&
                       neg[i].ts < t &&
                       (t - neg[i].ts) > tolerance)
                {
                    i++;
                }

                while (j < pos.Count &&
                       pos[j].ts < t &&
                       (t - pos[j].ts) > tolerance)
                {
                    j++;
                }

                if (i >= neg.Count ||
                    j >= pos.Count)
                {
                    break;
                }

                tn = neg[i].ts;
                tp = pos[j].ts;

                if (Math.Abs(
                        (tn - t).TotalMilliseconds) >
                    tolerance.TotalMilliseconds ||
                    Math.Abs(
                        (tp - t).TotalMilliseconds) >
                    tolerance.TotalMilliseconds)
                {
                    if (tn < tp)
                        i++;
                    else
                        j++;

                    continue;
                }

                var denominator = pos[j].mag;

                if (denominator > 0)
                {
                    output.Add((
                        t,
                        neg[i].mag / denominator));
                }

                i++;
                j++;
            }

            return output;
        }

        var series = new List<object>();

        var cachePoints =
            new List<(string pmuId, DateTime ts, double value)>();

        foreach (var group in rows.GroupBy(
                     r => r.IdName,
                     StringComparer.OrdinalIgnoreCase))
        {
            // Uma ordenação única por PMU; as seis listas já nascem ordenadas.
            var ordered = group
                .OrderBy(r => r.Ts)
                .ToList();

            if (ordered.Count == 0)
                continue;

            var vaMod = new List<(DateTime ts, double mag)>(ordered.Count);
            var vbMod = new List<(DateTime ts, double mag)>(ordered.Count);
            var vcMod = new List<(DateTime ts, double mag)>(ordered.Count);
            var vaAng = new List<(DateTime ts, double angDeg)>(ordered.Count);
            var vbAng = new List<(DateTime ts, double angDeg)>(ordered.Count);
            var vcAng = new List<(DateTime ts, double angDeg)>(ordered.Count);

            foreach (var frame in ordered)
            {
                if (frame.AMod.HasValue)
                    vaMod.Add((frame.Ts, frame.AMod.Value));

                if (frame.BMod.HasValue)
                    vbMod.Add((frame.Ts, frame.BMod.Value));

                if (frame.CMod.HasValue)
                    vcMod.Add((frame.Ts, frame.CMod.Value));

                if (frame.AAng.HasValue)
                    vaAng.Add((frame.Ts, frame.AAng.Value));

                if (frame.BAng.HasValue)
                    vbAng.Add((frame.Ts, frame.BAng.Value));

                if (frame.CAng.HasValue)
                    vcAng.Add((frame.Ts, frame.CAng.Value));
            }

            var first = ordered[0];

            if (vaMod.Count == 0 ||
                vbMod.Count == 0 ||
                vcMod.Count == 0 ||
                vaAng.Count == 0 ||
                vbAng.Count == 0 ||
                vcAng.Count == 0)
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new
                    {
                        kind,
                        metric = "unbalance"
                    },
                    points = Array.Empty<object>()
                });

                continue;
            }

            var seqPos =
                Sequences.ComputeSequenceMagnitudeMedPlot(
                    vaMod,
                    vbMod,
                    vcMod,
                    vaAng,
                    vbAng,
                    vcAng,
                    "pos");

            var seqNeg =
                Sequences.ComputeSequenceMagnitudeMedPlot(
                    vaMod,
                    vbMod,
                    vcMod,
                    vaAng,
                    vbAng,
                    vcAng,
                    "neg");

            if (seqPos.Count == 0 ||
                seqNeg.Count == 0)
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new
                    {
                        kind,
                        metric = "unbalance"
                    },
                    points = Array.Empty<object>()
                });

                continue;
            }

            const double EPS = 1e-12;

            if (!seqPos.Any(
                    p => Math.Abs(p.mag) > EPS))
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new
                    {
                        kind,
                        metric = "unbalance"
                    },
                    points = Array.Empty<object>()
                });

                continue;
            }

            var ratio = RatioPointwise(
                seqNeg
                    .Select(p => (p.ts, p.mag))
                    .ToList(),
                seqPos
                    .Select(p => (p.ts, p.mag))
                    .ToList(),
                TimeSpan.FromMilliseconds(3));

            if (ratio.Count == 0)
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new
                    {
                        kind,
                        metric = "unbalance"
                    },
                    points = Array.Empty<object>()
                });

                continue;
            }

            var ratioPercent =
                ratio
                    .Select(r => (
                        r.ts,
                        value: r.ratio * 100.0))
                    .ToList();

            foreach (var point in ratioPercent)
            {
                cachePoints.Add((
                    first.IdName,
                    point.ts,
                    point.value));
            }

            var points = _seriesAssembly.BuildPoints(
                ratio.Select(
                    p => (p.ts, p.ratio)),
                noDownsample: true,
                maxPoints: maxPts,
                downsampler: _down,
                outputScale: 100.0);

            series.Add(new
            {
                pmu = first.IdName,
                pdc = first.PdcName,
                unit = "percent",
                meta = new
                {
                    kind,
                    metric = "unbalance",
                    volt_level_kV =
                        first.VoltLevel is null
                            ? (double?)null
                            : first.VoltLevel.Value / 1000.0
                },
                points
            });
        }

        return new UnbalanceProjection(
            series,
            cachePoints,
            rows.Min(r => r.Ts),
            rows.Max(r => r.Ts));
    }

    private sealed record UnbalanceProjection(
        List<object> Series,
        List<(string pmuId, DateTime ts, double value)> CachePoints,
        DateTime WindowFrom,
        DateTime WindowTo);
}
