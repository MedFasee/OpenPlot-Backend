using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.BackgroundCache;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para series simples (frequency, dfreq, digital). Segue a mesma
/// arquitetura de CurrentSeriesHandler/SeqSeriesHandler: cache semantico via
/// IBackgroundCacheCoordinator/IBackgroundCacheQueue, sem Task.Run proprio.
/// </summary>
public sealed class SimpleSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly ITimeSeriesDownsampler _downsampler;
    private readonly IPlotMetaBuilder _meta;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly IUiMenuService _uiMenus;
    private readonly IBackgroundCacheQueue _backgroundCacheQueue;
    private readonly IBackgroundCacheCoordinator _backgroundCacheCoordinator;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly ILogger<SimpleSeriesHandler> _logger;

    public SimpleSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        ITimeSeriesDownsampler downsampler,
        IPlotMetaBuilder meta,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus,
        IBackgroundCacheQueue backgroundCacheQueue,
        IBackgroundCacheCoordinator backgroundCacheCoordinator,
        IHttpContextAccessor httpContextAccessor,
        ILogger<SimpleSeriesHandler> logger)
    {
        _runs = runs ?? throw new ArgumentNullException(nameof(runs));
        _meas = meas ?? throw new ArgumentNullException(nameof(meas));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _meta = meta ?? throw new ArgumentNullException(nameof(meta));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
        _backgroundCacheQueue = backgroundCacheQueue ?? throw new ArgumentNullException(nameof(backgroundCacheQueue));
        _backgroundCacheCoordinator = backgroundCacheCoordinator ?? throw new ArgumentNullException(nameof(backgroundCacheCoordinator));
        _httpContextAccessor = httpContextAccessor ?? throw new ArgumentNullException(nameof(httpContextAccessor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IResult> HandleAsync(
        SimpleSeriesQuery query,
        WindowQuery window,
        MeasurementsQuery meas,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        if (query.RunId == Guid.Empty)
            return Results.BadRequest("run_id é obrigatório.");

        if (window.FromUtc.HasValue && window.ToUtc.HasValue && window.FromUtc >= window.ToUtc)
            return Results.BadRequest("from deve ser menor que to.");

        var noDownsample = query.MaxPointsIsAll;
        var maxPts = query.ResolveMaxPoints(@default: 5000);

        var ctx = await _runs.ResolveAsync(query.RunId, window.FromUtc, window.ToUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var frontWatch = Stopwatch.StartNew();
        _logger.LogInformation(
            "[BYRUN][Simple][FRONT][START] runId={RunId} quantity={Quantity} component={Component} maxPoints={MaxPoints}",
            query.RunId,
            meas.Quantity,
            meas.Component,
            noDownsample ? "all" : maxPts);

        var frontRows = await _meas.QueryAsync(ctx, meas, ct, noDownsample ? null : maxPts);

        frontWatch.Stop();
        _logger.LogInformation(
            "[BYRUN][Simple][FRONT][END] runId={RunId} elapsedMs={ElapsedMs} rows={Rows}",
            query.RunId,
            frontWatch.ElapsedMilliseconds,
            frontRows.Count);

        if (frontRows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro.");

        var windowFrom = window.FromUtc ?? frontRows.Min(r => r.Ts);
        var windowTo = window.ToUtc ?? frontRows.Max(r => r.Ts);

        var cacheWorkKey = CacheWorkKey.Create(
            "Simple",
            query.RunId,
            window.FromUtc,
            window.ToUtc,
            ("quantity", meas.Quantity),
            ("component", meas.Component),
            ("pmus", CacheWorkKey.NormalizeCollection(meas.PmuNames)));

        var reservation = await _backgroundCacheCoordinator.ReserveOrGetAsync(cacheWorkKey, ct);
        var cacheId = reservation.CacheId;
        BackgroundCacheWorkItem? workItem = null;

        if (reservation.IsOwner && noDownsample)
        {
            var cacheSeriesAlreadyLoaded = BuildCacheSeries(frontRows, meas, _seriesAssembly);

            var payloadAlreadyLoaded = _seriesAssembly.BuildCachePayload(
                windowFrom,
                windowTo,
                ctx.SelectRate ?? 0,
                cacheSeriesAlreadyLoaded);

            workItem = new BackgroundCacheWorkItem(
                Name: "Simple",
                RunId: query.RunId,
                CacheId: cacheId,
                WorkKey: cacheWorkKey,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var cacheRepo = sp.GetRequiredService<IAnalysisCacheRepository>();
                    await cacheRepo.SaveAsync(cacheId, query.RunId, cacheWorkKey.CacheKey, payloadAlreadyLoaded, bgCt);
                });
        }
        else if (reservation.IsOwner)
        {
            var bgRunId = query.RunId;
            var bgFromUtc = window.FromUtc;
            var bgToUtc = window.ToUtc;
            var bgMeas = meas;

            workItem = new BackgroundCacheWorkItem(
                Name: "Simple",
                RunId: bgRunId,
                CacheId: cacheId,
                WorkKey: cacheWorkKey,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var runRepository = sp.GetRequiredService<IRunContextRepository>();
                    var measurementsRepository = sp.GetRequiredService<IMeasurementsRepository>();
                    var seriesAssembly = sp.GetRequiredService<ISeriesAssemblyService>();
                    var cacheRepo = sp.GetRequiredService<IAnalysisCacheRepository>();

                    var bgCtx = await runRepository.ResolveAsync(bgRunId, bgFromUtc, bgToUtc, bgCt);
                    if (bgCtx is null)
                        throw new InvalidOperationException(
                            $"Run não encontrado durante cache integral: {bgRunId}");

                    var fullRows = await measurementsRepository.QueryAsync(bgCtx, bgMeas, bgCt, maxPoints: null);
                    if (fullRows.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral Simple sem linhas. runId={bgRunId}");

                    var fullWindowFrom = bgFromUtc ?? fullRows.Min(r => r.Ts);
                    var fullWindowTo = bgToUtc ?? fullRows.Max(r => r.Ts);

                    var cacheSeriesFull = BuildCacheSeries(fullRows, bgMeas, seriesAssembly);

                    var fullPayload = seriesAssembly.BuildCachePayload(
                        fullWindowFrom,
                        fullWindowTo,
                        bgCtx.SelectRate ?? 0,
                        cacheSeriesFull);

                    await cacheRepo.SaveAsync(cacheId, bgRunId, cacheWorkKey.CacheKey, fullPayload, bgCt);
                });
        }

        if (reservation.IsOwner &&
            !_backgroundCacheQueue.ScheduleAfterResponse(_httpContextAccessor.HttpContext, workItem!))
        {
            await _backgroundCacheCoordinator.FailAsync(cacheWorkKey, cacheId, ct);
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
        }

        var series = frontRows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var first = g.First();
                var points = _seriesAssembly.BuildPoints(
                    g.Select(x => (x.Ts, x.Value)),
                    true,
                    maxPts,
                    _downsampler);

                return new SeriesDto(
                    Pdc: first.PdcName,
                    Pmu: first.IdName,
                    SignalId: first.SignalId,
                    PdcPmuId: first.PdcPmuId,
                    Unit: meas.Unit ?? "raw",
                    Meta: null,
                    Points: points);
            })
            .Cast<object>()
            .ToList();

        var plotMeta = _meta.Build(window, ctx, meas);

        var groupedRows = frontRows
            .GroupBy(row => row.IdName, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var pmuCount = groupedRows.Count;
        var availablePointCount = groupedRows.Count == 0 ? 0 : groupedRows.Min(g => g.Count());

        var menuContext = new UiMenuContext(
            WindowFromUtc: windowFrom,
            WindowToUtc: windowTo,
            SelectRate: ctx.SelectRate,
            TotalSeriesCount: pmuCount,
            ValidSeriesCount: pmuCount,
            AvailablePointCount: availablePointCount,
            Quantity: meas.Quantity,
            Component: meas.Component,
            Phase: meas.Phase);

        var resolvedModes = _uiMenus.RebuildForRun(modes, menuContext);

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(query.RunId, windowFrom, windowTo, series, plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(frontRows.First().PdcName, pmuCount)
            .Build();

        return Results.Ok(response);
    }

    private static List<RowsCacheSeries> BuildCacheSeries(
        IReadOnlyList<MeasurementRow> rows,
        MeasurementsQuery meas,
        ISeriesAssemblyService seriesAssembly) =>
        rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var first = g.First();
                return seriesAssembly.BuildCacheSeries(
                    signalId: first.SignalId,
                    pdcPmuId: first.PdcPmuId,
                    idName: first.IdName,
                    pdcName: first.PdcName,
                    referenceTerminal: null,
                    unit: meas.Unit,
                    phase: null,
                    quantity: meas.Quantity,
                    component: meas.Component,
                    points: g.Select(x => (x.Ts, x.Value)));
            })
            .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
            .ToList();
}

