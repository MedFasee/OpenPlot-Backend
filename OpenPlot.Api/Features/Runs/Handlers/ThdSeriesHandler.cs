using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para Distor��o Harm�nica Total (THD) de tens�o ou corrente.
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

    public ThdSeriesHandler(
        IRunContextRepository runRepository,
        IMeasurementsRepository measRepository,
        IAnalysisCacheRepository cacheRepository,
        ITimeSeriesDownsampler downsampler,
        IPlotMetaBuilder metaBuilder,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _measRepository = measRepository ?? throw new ArgumentNullException(nameof(measRepository));
        _cacheRepository = cacheRepository ?? throw new ArgumentNullException(nameof(cacheRepository));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _pmuHelper = pmuHelper ?? throw new ArgumentNullException(nameof(pmuHelper));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
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
        var noDownsample = false;

        // Quando tri=true, usa apenas query.Pmu (validado como obrigat�rio)
        // Quando tri=false, usa m�ltiplas PMUs de query.Pmus
        var pmuList = tri
            ? new[] { query.Pmu! }
            : _pmuHelper.Normalize(new[] { query.Pmu }, query.Pmus);

        DateTime? fromUtc = window.FromUtc;
        DateTime? toUtc = window.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepository.ResolveAsync(query.RunId, fromUtc, toUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id n�o encontrado.");

        var requestedMaxPoints = query.ResolveMaxPoints(@default: 5000);
        var estimatedPmuCount = tri
            ? 1
            : (pmuList.Length > 0 ? pmuList.Length : Math.Max(1, ctx.PmuNames.Count));
        var estimatedSeriesCount = Math.Max(1, estimatedPmuCount * (tri ? 3 : 1));
        var maxPts = SeriesDownsamplingPlanner.ResolveTargetMaxPointsPerSeries(
            requestedMaxPoints,
            query.MaxPointsIsAll,
            estimatedSeriesCount,
            ctx.FromUtc,
            ctx.ToUtc,
            ctx.SelectRate);

        // Constr�i a query de medi��es com filtros apropriados
        var phaseMode = tri ? PhaseMode.ABC : PhaseMode.Single;
        var measQuery = new MeasurementsQuery(
            Quantity: k == "voltage" ? "voltage" : "current",
            Component: "thd",
            PhaseMode: phaseMode,
            Phase: uphase,
            PmuNames: pmuList.Length == 0 ? null : pmuList,
            Unit: "%"
        );

        // Executa a query de medi��es (usa QueryPhasorAsync porque THD precisa da fase)
        var rows = await _measRepository.QueryPhasorAsync(ctx, measQuery, ct);

        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run_id/filtro no intervalo solicitado.");

        var windowFrom = rows.Min(r => r.Ts);
        var windowTo = rows.Max(r => r.Ts);

        // Constr�i as s�ries para cache
        var cacheSeries = rows
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

        RowsCacheV2? cachePayload = null;
        object? cacheId = null;
        if (!query.PreviewOnly)
        {
            cachePayload = _seriesAssembly.BuildCachePayload(
                windowFrom,
                windowTo,
                ctx.SelectRate ?? 0,
                cacheSeries);

            cacheId = await _cacheRepository.SaveAsync(query.RunId, cachePayload, ct);
        }

        // Constr�i as s�ries para resposta
        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var first = g.First();
                var points = _seriesAssembly.BuildPoints(
                    g.Select(r => (r.Ts, r.Value)),
                    noDownsample,
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
        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            cachePayload is not null
                ? UiMenuContext.FromCache(cachePayload)
                : new UiMenuContext(windowFrom, windowTo, ctx.SelectRate));

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
            return (false, "kind � obrigat�rio (voltage|current).");

        var k = kind.Trim().ToLowerInvariant();
        if (k is not ("voltage" or "current"))
            return (false, "kind deve ser 'voltage' ou 'current'.");

        var tri = query.Tri;
        if (!tri)
        {
            if (string.IsNullOrWhiteSpace(query.Phase))
                return (false, "phase � obrigat�rio (A|B|C) quando tri=false.");

            var phase = query.Phase.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(query.Pmu))
                return (false, "para tri=true � obrigat�rio informar pmu (id_name da PMU).");
        }

        return (true, null);
    }
}
