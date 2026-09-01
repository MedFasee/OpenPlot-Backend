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

public sealed class SeqSeriesHandler
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
    private readonly ILogger<SeqSeriesHandler> _logger;

    public SeqSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus,
        IBackgroundCacheQueue backgroundCacheQueue,
        IBackgroundCacheCoordinator backgroundCacheCoordinator,
        IHttpContextAccessor httpContextAccessor,
        ILogger<SeqSeriesHandler> logger)
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
        SeqRunQuery q,
        SeqRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "pu"))
            return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var ctx = await _runs.ResolveAsync(q.RunId, w.FromUtc, w.ToUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var kind = req.Kind == SeqKind.Current ? "current" : "voltage";

        _logger.LogInformation(
            "[BYRUN][SEQ][FRONT][START] runId={RunId} kind={Kind} maxPoints={MaxPoints}",
            q.RunId,
            kind,
            noDownsample ? "all" : maxPts);

        var frontWatch = Stopwatch.StartNew();

        var frontRows = await _meas.QueryAngleFramesAsync(
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
            "[BYRUN][SEQ][FRONT][END] runId={RunId} kind={Kind} elapsedMs={ElapsedMs} frames={Frames}",
            q.RunId,
            kind,
            frontWatch.ElapsedMilliseconds,
            frontRows.Count);

        if (frontRows.Count == 0)
            return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

        var projection = BuildSequenceSeries(
            frontRows,
            req,
            q,
            kind,
            maxPts,
            unit,
            buildFrontSeries: true,
            _seriesAssembly,
            _down);

        if (projection.Series.Count == 0)
            return Results.BadRequest("Nenhuma PMU pôde ser processada.");

        var cacheWorkKey = CacheWorkKey.Create(
            "Seq",
            q.RunId,
            w.FromUtc,
            w.ToUtc,
            ("kind", kind),
            ("seq", req.Seq.ToString()),
            ("unit", unit),
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
                    unit: unit,
                    phase: projection.SeqNorm,
                    quantity: kind,
                    component: "seq",
                    points: g.Select(x => (x.ts, x.value))))
                .ToList();

            var payloadAlreadyLoaded = _seriesAssembly.BuildCachePayload(
                projection.WindowFrom,
                projection.WindowTo,
                ctx.SelectRate ?? 0,
                cacheSeriesAlreadyLoaded);

            workItem = new BackgroundCacheWorkItem(
                Name: "Seq",
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
            var bgReq = req;
            var bgQuery = q;
            var bgKind = kind;
            var bgUnit = unit;

            workItem = new BackgroundCacheWorkItem(
                Name: "Seq",
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
                    var logger = sp.GetRequiredService<ILogger<SeqSeriesHandler>>();

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
                            $"Cache integral Seq sem frames. runId={bgRunId}");

                    var fullProjection = BuildSequenceSeries(
                        fullRows,
                        bgReq,
                        bgQuery,
                        bgKind,
                        int.MaxValue,
                        bgUnit,
                        buildFrontSeries: false,
                        seriesAssembly,
                        downsampler);

                    if (fullProjection.CachePoints.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral Seq sem pontos processados. runId={bgRunId}");

                    var cacheSeriesFull = fullProjection.CachePoints
                        .GroupBy(x => x.pmuId)
                        .Select(g => seriesAssembly.BuildCacheSeries(
                            signalId: 0,
                            pdcPmuId: 0,
                            idName: g.Key,
                            pdcName: bgCtx.PdcName,
                            referenceTerminal: null,
                            unit: bgUnit,
                            phase: fullProjection.SeqNorm,
                            quantity: bgKind,
                            component: "seq",
                            points: g.Select(x => (x.ts, x.value))))
                        .ToList();

                    var fullPayload = seriesAssembly.BuildCachePayload(
                        fullProjection.WindowFrom,
                        fullProjection.WindowTo,
                        bgCtx.SelectRate ?? 0,
                        cacheSeriesFull);

                    await cacheRepo.SaveAsync(cacheId, bgRunId, cacheWorkKey.CacheKey, fullPayload, bgCt);

                    logger.LogInformation(
                        "[BYRUN][SEQ][CACHE-FULL][PERSISTED] runId={RunId} cacheId={CacheId} frames={Frames}",
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

        var pmusForMeta = pmuList.Count == 0 ? null : pmuList;

        var seqMode = req.Seq switch
        {
            SeqType.Pos => PhaseMode.SeqPos,
            SeqType.Neg => PhaseMode.SeqNeg,
            _ => PhaseMode.SeqZero
        };

        var meas = new MeasurementsQuery(
            Quantity: kind,
            Component: "mag",
            PhaseMode: seqMode,
            PmuNames: pmusForMeta,
            Unit: unit);

        var plotMeta = _meta.Build(w, ctx, meas);

        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(
                WindowFromUtc: projection.WindowFrom,
                WindowToUtc: projection.WindowTo,
                SelectRate: ctx.SelectRate,
                TotalSeriesCount: projection.Series.Count,
                ValidSeriesCount: projection.Series.Count,
                Quantity: kind,
                Component: "seq",
                Phase: projection.SeqNorm));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(
                q.RunId,
                projection.WindowFrom,
                projection.WindowTo,
                projection.Series,
                plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, projection.Series.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unit,
                ["kind"] = kind,
                ["seq"] = projection.SeqNorm
            })
            .Build();

        return Results.Ok(response);
    }

    private static SequenceProjection BuildSequenceSeries(
        IReadOnlyList<AngleFrameRow> rows,
        SeqRequest req,
        SeqRunQuery q,
        string kind,
        int maxPts,
        string unit,
        bool buildFrontSeries,
        ISeriesAssemblyService seriesAssembly,
        ITimeSeriesDownsampler downsampler)
    {
        var seqNorm = req.Seq switch
        {
            SeqType.Pos => "pos",
            SeqType.Neg => "neg",
            _ => "zero"
        };

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

            if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
                vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            {
                continue;
            }

            var seqSeries = Sequences.ComputeSequenceMagnitudeMedPlot(
                vaMod,
                vbMod,
                vcMod,
                vaAng,
                vbAng,
                vcAng,
                seqNorm);

            if (seqSeries.Count == 0)
                continue;

            var first = ordered[0];
            double baseValue = 1.0;

            if (unit == "pu" && kind == "voltage")
            {
                var lvl = q.VoltLevel ?? first.VoltLevel ?? 0;
                if (lvl > 0)
                    baseValue = lvl / Math.Sqrt(3.0);
            }
            else if (unit == "pu" && kind == "current")
            {
                baseValue = 1.0;
            }

            double Unitize(double magnitude) =>
                unit == "pu" ? magnitude / baseValue : magnitude;

            var processedSeq = seqSeries
                .Select(p => (p.ts, value: Unitize(p.mag)))
                .ToList();

            foreach (var point in processedSeq)
                cachePoints.Add((first.IdName, point.ts, point.value));

            if (!buildFrontSeries)
                continue;

            var points = seriesAssembly.BuildPoints(
                processedSeq,
                noDownsample: true,
                maxPoints: maxPts,
                downsampler: downsampler);

            series.Add(new
            {
                pmu = first.IdName,
                pdc = first.PdcName,
                unit,
                meta = new
                {
                    kind,
                    seq = seqNorm,
                    volt_level_kV = first.VoltLevel is null
                        ? (double?)null
                        : first.VoltLevel.Value / 1000.0
                },
                points
            });
        }

        return new SequenceProjection(
            series,
            cachePoints,
            rows.Min(r => r.Ts),
            rows.Max(r => r.Ts),
            seqNorm);
    }

    private sealed record SequenceProjection(
        List<object> Series,
        List<(string pmuId, DateTime ts, double value)> CachePoints,
        DateTime WindowFrom,
        DateTime WindowTo,
        string SeqNorm);
}
