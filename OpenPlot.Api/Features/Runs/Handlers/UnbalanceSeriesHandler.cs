using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.BackgroundCache;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class UnbalanceSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IUiMenuService _uiMenus;
    private readonly IBackgroundCacheQueue _backgroundCacheQueue;
    private readonly IBackgroundCacheCoordinator _backgroundCacheCoordinator;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly ILogger<UnbalanceSeriesHandler> _logger;

    public UnbalanceSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus,
        IBackgroundCacheQueue backgroundCacheQueue,
        IBackgroundCacheCoordinator backgroundCacheCoordinator,
        IHttpContextAccessor httpContextAccessor,
        ILogger<UnbalanceSeriesHandler> logger)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _seriesAssembly = seriesAssembly;
        _uiMenus = uiMenus;
        _backgroundCacheQueue = backgroundCacheQueue;
        _backgroundCacheCoordinator = backgroundCacheCoordinator;
        _httpContextAccessor = httpContextAccessor;
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

        var ctx = await _runs.ResolveAsync(q.RunId, w.FromUtc, w.ToUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var kind = req.Kind == SeqKind.Current ? "current" : "voltage";

        _logger.LogInformation(
            "[BYRUN][UNBALANCE][FRONT][START] runId={RunId} kind={Kind} maxPoints={MaxPoints}",
            q.RunId,
            kind,
            noDownsample ? "all" : maxPts);

        var frontWatch = Stopwatch.StartNew();

        var rows = await _meas.QueryAngleFramesAsync(
            ctx,
            kind,
            pmuList.Count == 0 ? null : pmuList,
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
            return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

        var projection = BuildUnbalanceProjection(
            rows,
            kind,
            maxPts,
            buildFrontSeries: true,
            _seriesAssembly,
            _down);

        var series = projection.Series;
        var windowFrom = w.FromUtc ?? projection.WindowFrom;
        var windowTo = w.ToUtc ?? projection.WindowTo;

        var cacheWorkKey = CacheWorkKey.Create(
            "Unbalance",
            q.RunId,
            w.FromUtc,
            w.ToUtc,
            ("kind", kind),
            ("pmus", CacheWorkKey.NormalizeCollection(pmuList)));
        var reservation = await _backgroundCacheCoordinator.ReserveOrGetAsync(cacheWorkKey, ct);
        var cacheId = reservation.CacheId;
        BackgroundCacheWorkItem? workItem = null;

        if (reservation.IsOwner && noDownsample)
        {
            var cacheSeriesAlreadyLoaded = projection.CachePoints
                .GroupBy(x => x.pmuId)
                .Select(g => _seriesAssembly.BuildCacheSeries(
                    signalId: 0,
                    pdcPmuId: 0,
                    idName: g.Key,
                    pdcName: ctx.PdcName,
                    referenceTerminal: null,
                    unit: "%",
                    phase: null,
                    quantity: kind,
                    component: "ratio",
                    points: g.Select(x => (x.ts, x.value))))
                .ToList();

            var payloadAlreadyLoaded = _seriesAssembly.BuildCachePayload(
                windowFrom,
                windowTo,
                ctx.SelectRate ?? 0,
                cacheSeriesAlreadyLoaded);

            workItem = new BackgroundCacheWorkItem(
                Name: "Unbalance",
                RunId: q.RunId,
                CacheId: cacheId,
                WorkKey: cacheWorkKey,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var cacheRepo = sp.GetRequiredService<IAnalysisCacheRepository>();
                    await cacheRepo.SaveAsync(cacheId, q.RunId, cacheWorkKey.CacheKey, payloadAlreadyLoaded, bgCt);
                });
        }
        else if (reservation.IsOwner)
        {
            var bgRunId = q.RunId;
            var bgFromUtc = w.FromUtc;
            var bgToUtc = w.ToUtc;
            var bgPmus = pmuList.Count == 0 ? null : pmuList.ToArray();
            var bgKind = kind;

            workItem = new BackgroundCacheWorkItem(
                Name: "Unbalance",
                RunId: bgRunId,
                CacheId: cacheId,
                WorkKey: cacheWorkKey,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var runRepository = sp.GetRequiredService<IRunContextRepository>();
                    var measurementsRepository = sp.GetRequiredService<IMeasurementsRepository>();
                    var seriesAssembly = sp.GetRequiredService<ISeriesAssemblyService>();
                    var downsampler = sp.GetRequiredService<ITimeSeriesDownsampler>();
                    var cacheRepo = sp.GetRequiredService<IAnalysisCacheRepository>();
                    var logger = sp.GetRequiredService<ILogger<UnbalanceSeriesHandler>>();

                    var bgCtx = await runRepository.ResolveAsync(
                        bgRunId,
                        bgFromUtc,
                        bgToUtc,
                        bgCt);

                    if (bgCtx is null)
                        throw new InvalidOperationException(
                            $"Run não encontrado durante cache integral: {bgRunId}");

                    var fullRows = await measurementsRepository.QueryAngleFramesAsync(
                        bgCtx,
                        bgKind,
                        bgPmus,
                        bgFromUtc,
                        bgToUtc,
                        bgCt,
                        maxPoints: null,
                        phase: null);

                    if (fullRows.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral Unbalance sem frames. runId={bgRunId}");

                    var fullProjection = BuildUnbalanceProjection(
                        fullRows,
                        bgKind,
                        int.MaxValue,
                        buildFrontSeries: false,
                        seriesAssembly,
                        downsampler);

                    if (fullProjection.CachePoints.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral Unbalance sem pontos processados. runId={bgRunId}");

                    var cacheSeriesFull = fullProjection.CachePoints
                        .GroupBy(x => x.pmuId)
                        .Select(g => seriesAssembly.BuildCacheSeries(
                            signalId: 0,
                            pdcPmuId: 0,
                            idName: g.Key,
                            pdcName: bgCtx.PdcName,
                            referenceTerminal: null,
                            unit: "%",
                            phase: null,
                            quantity: bgKind,
                            component: "ratio",
                            points: g.Select(x => (x.ts, x.value))))
                        .ToList();

                    var fullPayload = seriesAssembly.BuildCachePayload(
                        bgFromUtc ?? fullProjection.WindowFrom,
                        bgToUtc ?? fullProjection.WindowTo,
                        bgCtx.SelectRate ?? 0,
                        cacheSeriesFull);

                    await cacheRepo.SaveAsync(cacheId, bgRunId, cacheWorkKey.CacheKey, fullPayload, bgCt);

                    logger.LogInformation(
                        "[BYRUN][UNBALANCE][CACHE-FULL][PERSISTED] runId={RunId} cacheId={CacheId} frames={Frames}",
                        bgRunId,
                        cacheId,
                        fullRows.Count);
                });
        }

        if (reservation.IsOwner &&
            !_backgroundCacheQueue.ScheduleAfterResponse(
                _httpContextAccessor.HttpContext,
                workItem!))
        {
            await _backgroundCacheCoordinator.FailAsync(cacheWorkKey, cacheId, ct);
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
        }

        var meas = new MeasurementsQuery(
            Quantity: kind,
            Component: "ratio",
            Unit: "%");

        var plotMeta = _meta.Build(w, ctx, meas);

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

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(
                q.RunId,
                windowFrom,
                windowTo,
                series,
                plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = "percent",
                ["kind"] = kind,
                ["metric"] = "unbalance"
            })
            .Build();

        return Results.Ok(response);
    }

    private static UnbalanceProjection BuildUnbalanceProjection(
        IReadOnlyList<AngleFrameRow> rows,
        string kind,
        int maxPts,
        bool buildFrontSeries,
        ISeriesAssemblyService seriesAssembly,
        ITimeSeriesDownsampler downsampler)
    {
        static List<(DateTime ts, double ratio)> RatioPointwise(
            List<(DateTime ts, double mag)> neg,
            List<(DateTime ts, double mag)> pos,
            TimeSpan tolerance)
        {
            var output = new List<(DateTime ts, double ratio)>();
            var i = 0;
            var j = 0;

            while (i < neg.Count && j < pos.Count)
            {
                var tn = neg[i].ts;
                var tp = pos[j].ts;
                var t = tn > tp ? tn : tp;

                while (i < neg.Count && neg[i].ts < t && (t - neg[i].ts) > tolerance)
                    i++;

                while (j < pos.Count && pos[j].ts < t && (t - pos[j].ts) > tolerance)
                    j++;

                if (i >= neg.Count || j >= pos.Count)
                    break;

                tn = neg[i].ts;
                tp = pos[j].ts;

                if (Math.Abs((tn - t).TotalMilliseconds) > tolerance.TotalMilliseconds ||
                    Math.Abs((tp - t).TotalMilliseconds) > tolerance.TotalMilliseconds)
                {
                    if (tn < tp)
                        i++;
                    else
                        j++;

                    continue;
                }

                var denominator = pos[j].mag;
                if (denominator > 0)
                    output.Add((t, neg[i].mag / denominator));

                i++;
                j++;
            }

            return output;
        }

        var series = new List<object>();
        var cachePoints = new List<(string pmuId, DateTime ts, double value)>();

        foreach (var group in rows.GroupBy(
                     r => r.IdName,
                     StringComparer.OrdinalIgnoreCase))
        {
            var ordered = group.OrderBy(r => r.Ts).ToList();
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
                if (frame.AMod.HasValue) vaMod.Add((frame.Ts, frame.AMod.Value));
                if (frame.BMod.HasValue) vbMod.Add((frame.Ts, frame.BMod.Value));
                if (frame.CMod.HasValue) vcMod.Add((frame.Ts, frame.CMod.Value));
                if (frame.AAng.HasValue) vaAng.Add((frame.Ts, frame.AAng.Value));
                if (frame.BAng.HasValue) vbAng.Add((frame.Ts, frame.BAng.Value));
                if (frame.CAng.HasValue) vcAng.Add((frame.Ts, frame.CAng.Value));
            }

            var first = ordered[0];

            if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
                vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            {
                if (buildFrontSeries)
                {
                    series.Add(new
                    {
                        pmu = first.IdName,
                        pdc = first.PdcName,
                        unit = "percent",
                        meta = new { kind, metric = "unbalance" },
                        points = Array.Empty<object>()
                    });
                }

                continue;
            }

            var seqPos = Sequences.ComputeSequenceMagnitudeMedPlot(
                vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, "pos");

            var seqNeg = Sequences.ComputeSequenceMagnitudeMedPlot(
                vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, "neg");

            if (seqPos.Count == 0 || seqNeg.Count == 0)
            {
                if (buildFrontSeries)
                {
                    series.Add(new
                    {
                        pmu = first.IdName,
                        pdc = first.PdcName,
                        unit = "percent",
                        meta = new { kind, metric = "unbalance" },
                        points = Array.Empty<object>()
                    });
                }

                continue;
            }

            const double EPS = 1e-12;
            if (!seqPos.Any(p => Math.Abs(p.mag) > EPS))
            {
                if (buildFrontSeries)
                {
                    series.Add(new
                    {
                        pmu = first.IdName,
                        pdc = first.PdcName,
                        unit = "percent",
                        meta = new { kind, metric = "unbalance" },
                        points = Array.Empty<object>()
                    });
                }

                continue;
            }

            var ratio = RatioPointwise(
                seqNeg.Select(p => (p.ts, p.mag)).ToList(),
                seqPos.Select(p => (p.ts, p.mag)).ToList(),
                TimeSpan.FromMilliseconds(3));

            if (ratio.Count == 0)
            {
                if (buildFrontSeries)
                {
                    series.Add(new
                    {
                        pmu = first.IdName,
                        pdc = first.PdcName,
                        unit = "percent",
                        meta = new { kind, metric = "unbalance" },
                        points = Array.Empty<object>()
                    });
                }

                continue;
            }

            var ratioPercent = ratio
                .Select(r => (r.ts, value: r.ratio * 100.0))
                .ToList();

            foreach (var point in ratioPercent)
                cachePoints.Add((first.IdName, point.ts, point.value));

            if (!buildFrontSeries)
                continue;

            var points = seriesAssembly.BuildPoints(
                ratio.Select(p => (p.ts, p.ratio)),
                noDownsample: true,
                maxPoints: maxPts,
                downsampler: downsampler,
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
                    volt_level_kV = first.VoltLevel is null
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
