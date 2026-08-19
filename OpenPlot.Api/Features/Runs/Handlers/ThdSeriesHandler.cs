using System.Diagnostics;
using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para Distorção Harmônica Total (THD) de tensão ou corrente.
/// Utiliza IMeasurementsRepository para filtragem consistente com outros handlers.
/// </summary>
public sealed class ThdSeriesHandler
{
    private readonly IRunContextRepository _runRepository;
    private readonly IMeasurementsRepository _measRepository;
    private readonly IAnalysisCacheRepository _cacheRepository;
    private readonly ITimeSeriesDownsampler _downsampler;
    private readonly IPlotMetaBuilder _metaBuilder;
    private readonly IPmuQueryHelper _pmuHelper;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly IUiMenuService _uiMenus;
    private readonly ILogger<ThdSeriesHandler> _logger;

    public ThdSeriesHandler(
        IRunContextRepository runRepository,
        IMeasurementsRepository measRepository,
        IAnalysisCacheRepository cacheRepository,
        ITimeSeriesDownsampler downsampler,
        IPlotMetaBuilder metaBuilder,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus,
        ILogger<ThdSeriesHandler> logger)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _measRepository = measRepository ?? throw new ArgumentNullException(nameof(measRepository));
        _cacheRepository = cacheRepository ?? throw new ArgumentNullException(nameof(cacheRepository));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _pmuHelper = pmuHelper ?? throw new ArgumentNullException(nameof(pmuHelper));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IResult> HandleAsync(
        ByRunQuery query,
        WindowQuery window,
        string kind,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var validation = ValidateThdInput(query, kind);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        var k = kind.Trim().ToLowerInvariant();
        var tri = query.Tri;
        var uphase = tri ? null : query.Phase?.Trim().ToUpperInvariant();
        var noDownsample = query.MaxPointsIsAll;
        var maxPts = query.ResolveMaxPoints(@default: 5000);

        // Quando tri=true, usa apenas query.Pmu (validado como obrigatório)
        // Quando tri=false, usa múltiplas PMUs de query.Pmus
        var pmuList = tri
            ? new[] { query.Pmu! }
            : _pmuHelper.Normalize(new[] { query.Pmu }, query.Pmus);

        DateTime? fromUtc = window.FromUtc;
        DateTime? toUtc = window.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepository.ResolveAsync(query.RunId, fromUtc, toUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        // Constrói a query de medições com filtros apropriados
        var phaseMode = tri ? PhaseMode.ABC : PhaseMode.Single;
        var measQuery = new MeasurementsQuery(
            Quantity: k == "voltage" ? "voltage" : "current",
            Component: "thd",
            PhaseMode: phaseMode,
            Phase: uphase,
            PmuNames: pmuList.Length == 0 ? null : pmuList,
            Unit: "%"
        );

        // Select rápido para front
        var frontWatch = Stopwatch.StartNew();
        _logger.LogInformation("[BYRUN][THD][FRONT][START] runId={RunId} kind={Kind} maxPoints={MaxPoints}", query.RunId, k, noDownsample ? "all" : maxPts);

        var frontRows = await _measRepository.QueryPhasorAsync(ctx, measQuery, ct, noDownsample ? null : maxPts);

        frontWatch.Stop();
        _logger.LogInformation("[BYRUN][THD][FRONT][END] runId={RunId} kind={Kind} elapsedMs={ElapsedMs} rows={Rows}", query.RunId, k, frontWatch.ElapsedMilliseconds, frontRows.Count);

        if (frontRows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run_id/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? frontRows.Min(r => r.Ts);
        var windowTo = toUtc ?? frontRows.Max(r => r.Ts);
        var cacheId = Guid.NewGuid();

        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;
            _logger.LogInformation("[BYRUN][THD][CACHE-BG][START] runId={RunId} kind={Kind} cacheId={CacheId}", query.RunId, k, cacheId);

            try
            {
                var fullRows = await _measRepository.QueryPhasorAsync(ctx, measQuery, CancellationToken.None, null);
                fullRowsCount = fullRows.Count;
                if (fullRowsCount == 0)
                    return;

                var cacheSeriesFull = fullRows
                    .GroupBy(r => r.SignalId)
                    .Select(g =>
                    {
                        var first = g.First();
                        return _seriesAssembly.BuildCacheSeries(
                            signalId: first.SignalId,
                            pdcPmuId: first.PdcPmuId,
                            idName: first.IdName,
                            pdcName: first.PdcName,
                            referenceTerminal: null,
                            unit: "%",
                            phase: first.Phase,
                            quantity: k,
                            component: first.Component,
                            points: g.Select(x => (x.Ts, x.Value)));
                    })
                    .ToList();

                var fullWindowFrom = fromUtc ?? fullRows.Min(r => r.Ts);
                var fullWindowTo = toUtc ?? fullRows.Max(r => r.Ts);

                var cachePayloadFull = _seriesAssembly.BuildCachePayload(
                    fullWindowFrom,
                    fullWindowTo,
                    ctx.SelectRate ?? 0,
                    cacheSeriesFull);

                await _cacheRepository.SaveAsync(cacheId, query.RunId, cachePayloadFull, CancellationToken.None);
                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao persistir cache assíncrono de thd/by-run. runId={RunId}", query.RunId);
            }
            finally
            {
                bgWatch.Stop();
                _logger.LogInformation("[BYRUN][THD][CACHE-BG][END] runId={RunId} kind={Kind} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}", query.RunId, k, cacheId, bgWatch.ElapsedMilliseconds, fullRowsCount, persisted);
            }
        });

        // Constrói as séries para resposta
        var series = frontRows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var first = g.First();
                var points = _seriesAssembly.BuildPoints(
                    g.Select(r => (r.Ts, r.Value)),
                    true,
                    maxPts,
                    _downsampler);

                return new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    signal_id = first.SignalId,
                    pdc_pmu_id = first.PdcPmuId,
                    meta = new
                    {
                        phase = first.Phase,
                        component = first.Component,
                        kind = k
                    },
                    points
                };
            })
            .ToList();

        var meas = new MeasurementsQuery(
            Quantity: k,
            Component: "thd",
            PhaseMode: phaseMode,
            Phase: uphase,
            PmuNames: pmuList.Length == 0 ? null : pmuList,
            Unit: "%"
        );

        var plotMeta = _metaBuilder.Build(window, ctx, meas);
        var selectedPmuCount = frontRows
            .Select(row => row.IdName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(
                WindowFromUtc: windowFrom,
                WindowToUtc: windowTo,
                SelectRate: ctx.SelectRate,
                TotalSeriesCount: selectedPmuCount,
                ValidSeriesCount: selectedPmuCount,
                Quantity: k,
                Component: "thd",
                Phase: tri ? "abc" : uphase?.Trim().ToLowerInvariant()));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(query.RunId, windowFrom, windowTo, series, plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Select(s => s.pmu).Distinct().Count())
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = "%",
                ["kind"] = k,
                ["tri"] = tri,
                ["phase"] = tri ? "ABC" : uphase
            })
            .Build();

        return Results.Ok(response);
    }

    private (bool isValid, string? errorMessage) ValidateThdInput(
        ByRunQuery query,
        string kind)
    {
        if (string.IsNullOrWhiteSpace(kind))
            return (false, "kind é obrigatório (voltage|current).");

        var k = kind.Trim().ToLowerInvariant();
        if (k is not ("voltage" or "current"))
            return (false, "kind deve ser 'voltage' ou 'current'.");

        var tri = query.Tri;
        if (!tri)
        {
            if (string.IsNullOrWhiteSpace(query.Phase))
                return (false, "phase é obrigatório (A|B|C) quando tri=false.");

            var phase = query.Phase.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(query.Pmu))
                return (false, "para tri=true é obrigatório informar pmu (id_name da PMU).");
        }

        return (true, null);
    }
}
