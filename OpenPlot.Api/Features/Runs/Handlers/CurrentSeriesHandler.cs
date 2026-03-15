using System.Collections.Generic;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Ui;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class CurrentSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly IPhasorRequestService _phasorRequest;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;

    public CurrentSeriesHandler(
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

    public Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, string[]? pmu, CancellationToken ct)
        => HandleAsync(q, w, pmu, modes: null, ct);

    // NOVO: recebe modes (já resolvido no endpoint)
    public async Task<IResult> HandleAsync(
        ByRunQuery q,
        WindowQuery w,
        string[]? pmu,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        static string PmuShort(string? idName)
        {
            if (string.IsNullOrWhiteSpace(idName)) return "";
            var s = idName.Trim();
            var i = s.IndexOf('|');
            return i >= 0 ? s[..i].Trim() : s;
        }

        var normalized = _phasorRequest.Resolve(q, pmu);
        if (!normalized.IsValid)
            return Results.BadRequest(normalized.Error);

        var selection = normalized.Selection!;
        var tri = selection.Tri;
        var pmuName = selection.TriPmuName;
        var uphase = selection.Phase;

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
            Quantity: "current",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri
                ? new[] { pmuName }
                : (pmuNames.Length > 0 ? pmuNames : null),
            Unit: "A"
        );

        var rows = await _meas.QueryPhasorAsync(ctx, meas, ct);
        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);

        // ===== CACHE =====
        var cacheSeries = rows
            .GroupBy(r => new
            {
                r.SignalId,
                Phase = (r.Phase ?? "").Trim(),
                Component = (r.Component ?? "").Trim(),
                r.PdcPmuId,
                r.IdName,
                r.PdcName
            })
            .Select(g =>
            {
                var first = g.First();
                return _seriesAssembly.BuildCacheSeries(
                    signalId: first.SignalId,
                    pdcPmuId: first.PdcPmuId,
                    idName: first.IdName,
                    pdcName: first.PdcName,
                    unit: "A",
                    phase: first.Phase,
                    quantity: "current",
                    component: first.Component,
                    points: g.Select(x => (x.Ts, x.Value)));
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
        // =======================================================

        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                var points = _seriesAssembly.BuildPoints(
                    g.Select(x => (x.Ts, x.Value)),
                    noDownsample,
                    maxPts,
                    _down);

                return new
                {
                    pmu = PmuShort(any.IdName), // <<< até o primeiro '|'
                    pdc = any.PdcName,
                    signal_id = any.SignalId,
                    pdc_pmu_id = any.PdcPmuId,
                    meta = new
                    {
                        phase = (any.Phase ?? "").Trim().ToUpperInvariant(),
                        component = (any.Component ?? "").Trim().ToUpperInvariant()
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
            .WithResolved(ctx.PdcName, series.Select(s => s.pmu).Distinct(StringComparer.OrdinalIgnoreCase).Count())
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = "raw",
                ["tri"] = tri,
                ["phase"] = tri ? "ABC" : uphase
            })
            .Build();

        return Results.Ok(response);
    }
}