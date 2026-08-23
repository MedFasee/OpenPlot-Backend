using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using OpenPlot.Services.BackgroundCache;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Potência ativa/reativa calculada sobre frames Wide sincronizados.
///
/// A leitura de V e I é feita em UMA única consulta por janela:
/// measurements_wide_2 -> time_bucket -> V/I ABC MAG+ANG -> cálculo P/Q.
/// </summary>
public sealed class PowerSeriesHandler
{
    private readonly IMeasurementsRepository _measurementsRepository;
    private readonly IRunContextRepository _runRepository;
    private readonly ILogger<PowerSeriesHandler> _logger;
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IPlotMetaBuilder _metaBuilder;
    private readonly IPmuQueryHelper _pmuHelper;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly IUiMenuService _uiMenus;
    private readonly IBackgroundCacheQueue _backgroundCacheQueue;
    private readonly IHttpContextAccessor _httpContextAccessor;

    public PowerSeriesHandler(
        IRunContextRepository runRepository,
        IDbConnectionFactory dbFactory,
        IMeasurementsRepository measurementsRepository,
        IAnalysisCacheRepository cacheRepo,
        IPlotMetaBuilder metaBuilder,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus,
        IBackgroundCacheQueue backgroundCacheQueue,
        IHttpContextAccessor httpContextAccessor,
        ILogger<PowerSeriesHandler> logger)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _ = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory)); // preserva assinatura DI atual
        _measurementsRepository = measurementsRepository ?? throw new ArgumentNullException(nameof(measurementsRepository));
        _cacheRepo = cacheRepo ?? throw new ArgumentNullException(nameof(cacheRepo));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _pmuHelper = pmuHelper ?? throw new ArgumentNullException(nameof(pmuHelper));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
        _backgroundCacheQueue = backgroundCacheQueue ?? throw new ArgumentNullException(nameof(backgroundCacheQueue));
        _httpContextAccessor = httpContextAccessor ?? throw new ArgumentNullException(nameof(httpContextAccessor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IResult> HandleAsync(
        PowerPlotQuery query,
        WindowQuery window,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var validation = ValidatePowerQuery(query);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        var processingWatch = Stopwatch.StartNew();

        var which = (query.Which ?? "active").Trim().ToLowerInvariant();
        var quantityKey = which == "active" ? "p_active" : "p_reactive";
        var unit = (query.Unit ?? "raw").Trim().ToLowerInvariant();
        var maxPts = Math.Max(query.ResolveMaxPoints(@default: 100), 100);

        _logger.LogInformation(
            "[PROCESS][Power][START] runId={RunId} which={Which} tri={Tri} total={Total} phase={Phase}",
            query.RunId,
            which,
            query.Tri,
            query.Total,
            query.Phase);

        var fromUtc = window.FromUtc;
        var toUtc = window.ToUtc;

        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepository.ResolveAsync(
            query.RunId,
            fromUtc,
            toUtc,
            ct);

        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var pmuList = _pmuHelper.Normalize(query.Pmu).ToList();
        IReadOnlyList<string>? pmuFilter = pmuList.Count > 0 ? pmuList : null;

        var tri = query.Tri ?? false;
        var total = query.Total ?? false;
        var phase = !tri && !total
            ? query.Phase?.Trim().ToUpperInvariant()
            : null;

        int? frontMaxPoints = query.MaxPointsIsAll
        ? null
        : maxPts;

        // UMA única leitura da Wide para V + I.
        var frames = await _measurementsRepository.QueryPowerFramesAsync(
            ctx,
            pmuFilter,
            fromUtc,
            toUtc,
            ct,
            frontMaxPoints);

        if (frames.Count == 0)
            return Results.NotFound(
                "Nada encontrado para esse run/filtro no intervalo solicitado.");

        var projection = BuildPowerProjection(
            frames,
            tri,
            total,
            phase,
            which,
            unit,
            buildFrontSeries: true);

        var seriesOut = projection.series;
        var cachePoints = projection.cachePoints;

        if (seriesOut.Count == 0)
            return Results.BadRequest(
                "Nenhuma PMU pôde ser processada (faltam V/I MAG+ANG na linha Wide).");

        var windowFrom = fromUtc ?? frames.Min(r => r.Ts);
        var windowTo = toUtc ?? frames.Max(r => r.Ts);

        var unitDisplay = unit == "mw"
            ? (which == "active" ? "MW" : "MVAr")
            : "raw";

        var cacheSeries = cachePoints
            .GroupBy(x => x.pmuId)
            .Select(g => _seriesAssembly.BuildCacheSeries(
                signalId: 0,
                pdcPmuId: 0,
                idName: g.Key,
                pdcName: ctx.PdcName,
                referenceTerminal: null,
                unit: unitDisplay,
                phase: null,
                quantity: which,
                component: "power",
                points: g.Select(x => (x.ts, x.value))))
            .ToList();

        var cachePayload = _seriesAssembly.BuildCachePayload(
            windowFrom,
            windowTo,
            ctx.SelectRate ?? 0,
            cacheSeries,
            normalizeMissingFrames: false);

        var cacheId = Guid.NewGuid();

        // Não persistimos mais o preview no caminho crítico.
        // O cache_id fica "pending" até o worker persistir a massa integral.
        //
        // IMPORTANTE:
        // - preview: maxPoints aplicado no banco;
        // - BG: maxPoints=null => RAW, sem downsample;
        // - o BG só entra na fila após a resposta HTTP terminar.
        var bgRunId = query.RunId;
        var bgFromUtc = fromUtc;
        var bgToUtc = toUtc;
        var bgPmus = pmuFilter?.ToArray();
        var bgTri = tri;
        var bgTotal = total;
        var bgPhase = phase;
        var bgWhich = which;
        var bgUnit = unit;
        var bgUnitDisplay = unitDisplay;

        BackgroundCacheWorkItem workItem;

        if (query.MaxPointsIsAll)
        {
            // Quando o próprio request já pediu massa integral, não consultamos
            // o banco novamente. Apenas persistimos o payload integral já montado.
            var fullPayloadAlreadyLoaded = cachePayload;

            workItem = new BackgroundCacheWorkItem(
                Name: "Power",
                RunId: bgRunId,
                CacheId: cacheId,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var cacheRepo =
                        sp.GetRequiredService<IAnalysisCacheRepository>();

                    await cacheRepo.SaveAsync(
                        cacheId,
                        bgRunId,
                        fullPayloadAlreadyLoaded,
                        bgCt);
                });
        }
        else
        {
            workItem = new BackgroundCacheWorkItem(
                Name: "Power",
                RunId: bgRunId,
                CacheId: cacheId,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var runRepository =
                        sp.GetRequiredService<IRunContextRepository>();

                    var measurementsRepository =
                        sp.GetRequiredService<IMeasurementsRepository>();

                    var seriesAssembly =
                        sp.GetRequiredService<ISeriesAssemblyService>();

                    var cacheRepo =
                        sp.GetRequiredService<IAnalysisCacheRepository>();

                    var logger =
                        sp.GetRequiredService<ILogger<PowerSeriesHandler>>();

                    var bgCtx = await runRepository.ResolveAsync(
                        bgRunId,
                        bgFromUtc,
                        bgToUtc,
                        bgCt);

                    if (bgCtx is null)
                        throw new InvalidOperationException(
                            $"Run não encontrado durante cache integral: {bgRunId}");

                    var fullFrames =
                        await measurementsRepository.QueryPowerFramesAsync(
                            bgCtx,
                            bgPmus,
                            bgFromUtc,
                            bgToUtc,
                            bgCt,
                            maxPoints: null);

                    if (fullFrames.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral Power sem frames. runId={bgRunId}");

                    var fullProjection = BuildPowerProjection(
                        fullFrames,
                        bgTri,
                        bgTotal,
                        bgPhase,
                        bgWhich,
                        bgUnit,
                        buildFrontSeries: false);

                    if (fullProjection.cachePoints.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral Power sem pontos processados. runId={bgRunId}");

                    var fullWindowFrom =
                        bgFromUtc ?? fullFrames.Min(x => x.Ts);

                    var fullWindowTo =
                        bgToUtc ?? fullFrames.Max(x => x.Ts);

                    var cacheSeriesFull =
                        fullProjection.cachePoints
                            .GroupBy(x => x.pmuId)
                            .Select(g => seriesAssembly.BuildCacheSeries(
                                signalId: 0,
                                pdcPmuId: 0,
                                idName: g.Key,
                                pdcName: bgCtx.PdcName,
                                referenceTerminal: null,
                                unit: bgUnitDisplay,
                                phase: null,
                                quantity: bgWhich,
                                component: "power",
                                points: g.Select(x => (x.ts, x.value))))
                            .ToList();

                    // DEFAULT normalizeMissingFrames=true:
                    // não é downsampling; apenas preserva a política de
                    // preenchimento de frames faltantes do RowsCacheV2.
                    var fullPayload =
                        seriesAssembly.BuildCachePayload(
                            fullWindowFrom,
                            fullWindowTo,
                            bgCtx.SelectRate ?? 0,
                            cacheSeriesFull);

                    await cacheRepo.SaveAsync(
                        cacheId,
                        bgRunId,
                        fullPayload,
                        bgCt);

                    logger.LogInformation(
                        "[BYRUN][Power][CACHE-FULL][PERSISTED] runId={RunId} cacheId={CacheId} frames={Frames}",
                        bgRunId,
                        cacheId,
                        fullFrames.Count);
                });
        }

        if (!_backgroundCacheQueue.ScheduleAfterResponse(
                _httpContextAccessor.HttpContext,
                workItem))
        {
            return Results.StatusCode(
                StatusCodes.Status503ServiceUnavailable);
        }

        var meas = new MeasurementsQuery(
            Quantity: quantityKey,
            Component: "power",
            PhaseMode: PhaseMode.Any,
            Unit: unitDisplay);

        var plotMeta = _metaBuilder.Build(window, ctx, meas);

        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            UiMenuContext.FromCache(cachePayload));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(
                query.RunId,
                windowFrom,
                windowTo,
                seriesOut,
                plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, seriesOut.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unitDisplay,
                ["type"] = which,
                ["tri"] = tri,
                ["total"] = total,
                ["phase"] = phase
            })
            .Build();

        processingWatch.Stop();

        _logger.LogInformation(
            "[PROCESS][Power][END] runId={RunId} elapsedMs={ElapsedMs} frames={Frames} series={SeriesCount}",
            query.RunId,
            processingWatch.ElapsedMilliseconds,
            frames.Count,
            seriesOut.Count);

        return Results.Ok(response);
    }

    private static (bool isValid, string? errorMessage) ValidatePowerQuery(
        PowerPlotQuery query)
    {
        if (query.RunId == Guid.Empty)
            return (false, "run_id é obrigatório.");

        var which = (query.Which ?? "active").Trim().ToLowerInvariant();
        if (which is not ("active" or "reactive"))
            return (false, "which deve ser 'active' ou 'reactive'.");

        var unit = (query.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "mw"))
            return (false, "unit deve ser 'raw' ou 'mw'.");

        var tri = query.Tri ?? false;
        var total = query.Total ?? false;

        if (tri && total)
            return (false, "tri=true e total=true são mutuamente exclusivos.");

        if (tri && (query.Pmu?.Length ?? 0) != 1)
            return (false, "tri=true exige exatamente 1 pmu (id_name).");

        if (!tri && !total && string.IsNullOrWhiteSpace(query.Phase))
            return (false, "phase é obrigatório quando tri=false e total=false.");

        if (!tri && !total)
        {
            var phase = query.Phase?.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }

        return (true, null);
    }

    private static (
        List<object> series,
        List<(string pmuId, DateTime ts, double value)> cachePoints)
        BuildPowerProjection(
            IReadOnlyList<PowerFrameRow> frames,
            bool tri,
            bool total,
            string? phase,
            string which,
            string unit,
            bool buildFrontSeries)
    {
        var series = new List<object>();
        var cachePoints = new List<(string pmuId, DateTime ts, double value)>();

        var unitDisplay = unit == "mw"
            ? (which == "active" ? "MW" : "MVAr")
            : "raw";

        // Mantém a mesma escala do handler anterior.
        const double scale = 1e-6;

        foreach (var pmuGroup in frames.GroupBy(
                     x => x.IdName,
                     StringComparer.OrdinalIgnoreCase))
        {
            var ordered = pmuGroup
                .OrderBy(x => x.Ts)
                .ToList();

            if (ordered.Count == 0)
                continue;

            var any = ordered[0];

            List<(DateTime ts, double val)> PhasePoints(string ph)
            {
                var output = new List<(DateTime ts, double val)>(ordered.Count);

                foreach (var frame in ordered)
                {
                    var power = ph switch
                    {
                        "A" => CalculatePower(
                            frame.VaMod, frame.VaAng,
                            frame.IaMod, frame.IaAng,
                            which),

                        "B" => CalculatePower(
                            frame.VbMod, frame.VbAng,
                            frame.IbMod, frame.IbAng,
                            which),

                        "C" => CalculatePower(
                            frame.VcMod, frame.VcAng,
                            frame.IcMod, frame.IcAng,
                            which),

                        _ => null
                    };

                    if (power.HasValue)
                        output.Add((frame.Ts, power.Value));
                }

                return output;
            }

            if (tri)
            {
                foreach (var ph in new[] { "A", "B", "C" })
                {
                    var pts = PhasePoints(ph);
                    if (pts.Count == 0)
                        continue;

                    var scaled = pts
                        .Select(x => (x.ts, val: x.val * scale))
                        .ToList();

                    foreach (var p in scaled)
                        cachePoints.Add((any.IdName, p.ts, p.val));

                    if (!buildFrontSeries)
                        continue;

                    series.Add(new
                    {
                        pmu = any.IdName,
                        pdc = any.PdcName,
                        meta = new { phase = ph },
                        unit = unitDisplay,
                        points = scaled.Select(p => new object[] { p.ts, p.val })
                    });
                }

                continue;
            }

            if (total)
            {
                var sum = new List<(DateTime ts, double val)>(ordered.Count);

                foreach (var frame in ordered)
                {
                    var pa = CalculatePower(
                        frame.VaMod, frame.VaAng,
                        frame.IaMod, frame.IaAng,
                        which);

                    var pb = CalculatePower(
                        frame.VbMod, frame.VbAng,
                        frame.IbMod, frame.IbAng,
                        which);

                    var pc = CalculatePower(
                        frame.VcMod, frame.VcAng,
                        frame.IcMod, frame.IcAng,
                        which);

                    if (pa.HasValue && pb.HasValue && pc.HasValue)
                        sum.Add((frame.Ts, pa.Value + pb.Value + pc.Value));
                }

                if (sum.Count == 0)
                    continue;

                var scaled = sum
                    .Select(x => (x.ts, val: x.val * scale))
                    .ToList();

                foreach (var p in scaled)
                    cachePoints.Add((any.IdName, p.ts, p.val));

                if (buildFrontSeries)
                {
                    series.Add(new
                    {
                        pmu = any.IdName,
                        pdc = any.PdcName,
                        meta = new { total = true },
                        unit = unitDisplay,
                        points = scaled.Select(p => new object[] { p.ts, p.val })
                    });
                }

                continue;
            }

            var single = PhasePoints(phase!);
            if (single.Count == 0)
                continue;

            var singleScaled = single
                .Select(x => (x.ts, val: x.val * scale))
                .ToList();

            foreach (var p in singleScaled)
                cachePoints.Add((any.IdName, p.ts, p.val));

            if (buildFrontSeries)
            {
                series.Add(new
                {
                    pmu = any.IdName,
                    pdc = any.PdcName,
                    meta = new { phase },
                    unit = unitDisplay,
                    points = singleScaled.Select(p => new object[] { p.ts, p.val })
                });
            }
        }

        return (series, cachePoints);
    }

    private static double? CalculatePower(
        double? vMag,
        double? vAngDeg,
        double? iMag,
        double? iAngDeg,
        string which)
    {
        if (!vMag.HasValue ||
            !vAngDeg.HasValue ||
            !iMag.HasValue ||
            !iAngDeg.HasValue)
        {
            return null;
        }

        const double Deg2Rad = Math.PI / 180.0;

        var apparent = vMag.Value * iMag.Value;
        var angleDiff = (vAngDeg.Value - iAngDeg.Value) * Deg2Rad;

        return which == "active"
            ? apparent * Math.Cos(angleDiff)
            : apparent * Math.Sin(angleDiff);
    }
}