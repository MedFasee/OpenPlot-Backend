using System.Diagnostics;
using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class VoltageSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly IPhasorRequestService _phasorRequest;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IUiMenuService _uiMenus;
    private readonly ILogger<VoltageSeriesHandler> _logger;

    public VoltageSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        IPhasorRequestService phasorRequest,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepo,
        IUiMenuService uiMenus,
        ILogger<VoltageSeriesHandler> logger)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _phasorRequest = phasorRequest;
        _seriesAssembly = seriesAssembly;
        _cacheRepo = cacheRepo;
        _uiMenus = uiMenus;
        _logger = logger;
    }

    // Recebe UI já resolvida no endpoint
    public async Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, string[]? pmu, Dictionary<string, object?>? modes, CancellationToken ct)
    {
        var normalized = _phasorRequest.Resolve(q, pmu);
        if (!normalized.IsValid)
            return Results.BadRequest(normalized.Error);

        var selection = normalized.Selection!;
        var tri = selection.Tri;
        var pmuName = selection.TriPmuName;
        var uphase = selection.Phase;

        var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "pu"))
            return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runs.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var pmuNames = selection.PmuNames;

        var meas = new MeasurementsQuery(
            Quantity: "voltage",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri
                ? new[] { pmuName }
                : (pmuNames.Length > 0 ? pmuNames : null),
            Unit: unit
        );

        var frontWatch = Stopwatch.StartNew();
        _logger.LogInformation("[BYRUN][Voltage][FRONT][START] runId={RunId} maxPoints={MaxPoints}", q.RunId, noDownsample ? "all" : maxPts);

        var frontRows = await _meas.QueryPhasorAsync(ctx, meas, ct, noDownsample ? null : maxPts);

        frontWatch.Stop();
        _logger.LogInformation("[BYRUN][Voltage][FRONT][END] runId={RunId} elapsedMs={ElapsedMs} rows={Rows}", q.RunId, frontWatch.ElapsedMilliseconds, frontRows.Count);

        if (frontRows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? frontRows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? frontRows.Max(r => r.Ts);
        var cacheId = Guid.NewGuid();

        // ===== PROCESSAMENTO DE UNIDADE PARA FRONT =====
        var frontProcessedData = unit == "pu"
            ? frontRows.Select(r => (r, value: PerUnit.ToVoltagePu(r.Value, r.VoltLevel))).ToList()
            : frontRows.Select(r => (r, value: r.Value)).ToList();

        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;
            _logger.LogInformation("[BYRUN][Voltage][CACHE-BG][START] runId={RunId} cacheId={CacheId}", q.RunId, cacheId);

            try
            {
                var fullRows = await _meas.QueryPhasorAsync(ctx, meas, CancellationToken.None, null);
                fullRowsCount = fullRows.Count;
                if (fullRowsCount == 0)
                    return;

                var fullWindowFrom = fromUtc ?? fullRows.Min(r => r.Ts);
                var fullWindowTo = toUtc ?? fullRows.Max(r => r.Ts);

                var fullProcessed = unit == "pu"
                    ? fullRows.Select(r => (r, value: PerUnit.ToVoltagePu(r.Value, r.VoltLevel))).ToList()
                    : fullRows.Select(r => (r, value: r.Value)).ToList();

                var cacheSeriesFull = fullProcessed
                    .GroupBy(x => new
                    {
                        x.r.SignalId,
                        Phase = (x.r.Phase ?? "").Trim(),
                        Component = (x.r.Component ?? "").Trim(),
                        x.r.PdcPmuId,
                        x.r.IdName,
                        x.r.PdcName
                    })
                    .Select(g =>
                    {
                        var first = g.First();
                        return _seriesAssembly.BuildCacheSeries(
                            signalId: first.r.SignalId,
                            pdcPmuId: first.r.PdcPmuId,
                            idName: first.r.IdName,
                            pdcName: first.r.PdcName,
                            referenceTerminal: null,
                            unit: unit,
                            phase: first.r.Phase,
                            quantity: "voltage",
                            component: first.r.Component,
                            points: g.Select(x => (x.r.Ts, x.value)));
                    })
                    .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var cachePayloadFull = _seriesAssembly.BuildCachePayload(
                    fullWindowFrom,
                    fullWindowTo,
                    ctx.SelectRate ?? 0,
                    cacheSeriesFull);

                await _cacheRepo.SaveAsync(cacheId, q.RunId, cachePayloadFull, CancellationToken.None);
                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao persistir cache assíncrono de voltage/by-run. runId={RunId}", q.RunId);
            }
            finally
            {
                bgWatch.Stop();
                _logger.LogInformation("[BYRUN][Voltage][CACHE-BG][END] runId={RunId} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}", q.RunId, cacheId, bgWatch.ElapsedMilliseconds, fullRowsCount, persisted);
            }
        });

        var series = frontProcessedData
            .GroupBy(x => x.r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                var points = _seriesAssembly.BuildPoints(
                    g.Select(x => (x.r.Ts, x.value)),
                    true,
                    maxPts,
                    _down);

                return new
                {
                    pmu = any.r.IdName,
                    pdc = any.r.PdcName,
                    signal_id = any.r.SignalId,
                    pdc_pmu_id = any.r.PdcPmuId,
                    meta = new
                    {
                        phase = (any.r.Phase ?? "").Trim().ToUpperInvariant(),
                        component = (any.r.Component ?? "").Trim().ToUpperInvariant(),
                        volt_level_kV = any.r.VoltLevel is null ? (double?)null : any.r.VoltLevel.Value / 1000.0
                    },
                    points
                };
            })
            .ToList();


        var plotMeta = _meta.Build(w, ctx, meas);
        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(windowFrom, windowTo2, ctx.SelectRate));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo2, series, plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Select(s => s.pmu).Distinct().Count())
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unit,
                ["tri"] = tri,
                ["phase"] = tri ? "ABC" : uphase
            })
            .Build();

        return Results.Ok(response);
    }
}