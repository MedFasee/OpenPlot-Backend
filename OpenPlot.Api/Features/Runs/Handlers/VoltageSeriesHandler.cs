using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Features.Ui;

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

    public VoltageSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        IPhasorRequestService phasorRequest,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepo)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _phasorRequest = phasorRequest;
        _seriesAssembly = seriesAssembly;
        _cacheRepo = cacheRepo;
    }

    // Mantém compatibilidade (chamadas antigas)
    public Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, CancellationToken ct)
        => HandleAsync(q, w, pmu: null, modes: null, ct);

    // Mantém compatibilidade (chamadas antigas)
    public Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, string[]? pmu, CancellationToken ct)
        => HandleAsync(q, w, pmu, modes: null, ct);

    // NOVO: recebe UI (já resolvida no endpoint)
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

        var rows = await _meas.QueryPhasorAsync(ctx, meas, ct);
        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);

        // ===== PROCESSAMENTO DE UNIDADE ANTES DO CACHE =====
        // Se unit == "pu", converte os valores agora (antes de armazenar em cache)
        var processedData = unit == "pu"
            ? rows.Select(r => (r, value: PerUnit.ToVoltagePu(r.Value, r.VoltLevel))).ToList()
            : rows.Select(r => (r, value: r.Value)).ToList();

        var cacheSeries = processedData
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

        var cachePayload = _seriesAssembly.BuildCachePayload(
            windowFrom,
            windowTo2,
            (int)ctx.SelectRate,
            cacheSeries);

        var cacheId = await _cacheRepo.SaveAsync(q.RunId, cachePayload, ct);

        // ===== DOWNSAMPLING DEPOIS (PARA VISUALIZAÇÃO) =====
        var series = processedData
            .GroupBy(x => x.r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                var points = _seriesAssembly.BuildPoints(
                    g.Select(x => (x.r.Ts, x.value)),
                    noDownsample,
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

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo2, series, plotMeta)
            .WithModes(modes)
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