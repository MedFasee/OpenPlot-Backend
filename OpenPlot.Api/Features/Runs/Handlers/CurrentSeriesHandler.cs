using System.Globalization;
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
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;

    public CurrentSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        IAnalysisCacheRepository cacheRepo)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
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

        var tri = q.Tri;

        // tri=true: precisa de 1 PMU (usa q.Pmu; se vier via pmu[]=..., usa o primeiro)
        var pmuName = q.Pmu?.Trim();
        if (tri && string.IsNullOrWhiteSpace(pmuName) && pmu is { Length: > 0 })
            pmuName = pmu[0]?.Trim();

        string? uphase = null;

        if (!tri)
        {
            if (string.IsNullOrWhiteSpace(q.Phase))
                return Results.BadRequest("phase é obrigatório (A|B|C) quando tri=false.");

            uphase = q.Phase.Trim().ToUpperInvariant();
            if (uphase is not ("A" or "B" or "C"))
                return Results.BadRequest("phase deve ser A, B ou C.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(pmuName))
                return Results.BadRequest("para tri=true é obrigatório informar pmu (id_name da PMU).");
        }

        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runs.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        // tri=false: usa pmu[] do endpoint se vier; senão usa q.Pmus (se existir)
        var pmuNames = (pmu ?? q.Pmus ?? Array.Empty<string>())
            .Select(x => x?.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var meas = new MeasurementsQuery(
            Quantity: "current",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri
                ? new[] { pmuName! }
                : (pmuNames.Length > 0 ? pmuNames : null),
            Unit: "A"
        );

        var rows = await _meas.QueryPhasorAsync(ctx, meas, ct);
        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);

        // ===== CACHE =====
        var cachePayload = new RowsCacheV2
        {
            From = windowFrom.ToUniversalTime(),
            To = windowTo2.ToUniversalTime(),
            SelectRate = (int)ctx.SelectRate,

            Series = rows
                // chave composta: separa A/B/C, MAG/ANG etc.
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

                    return new RowsCacheSeries
                    {
                        SignalId = first.SignalId,
                        PdcPmuId = first.PdcPmuId,
                        IdName = first.IdName,
                        PdcName = first.PdcName,

                        Unit = "A",
                        Phase = first.Phase,
                        Quantity = "current",
                        Component = first.Component,

                        Points = g
                            .OrderBy(x => x.Ts)
                            .Select(x => new RowsCachePoint
                            {
                                Ts = x.Ts.ToUniversalTime(),
                                Value = x.Value
                            })
                            .ToList()
                    };
                })
                .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
                .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
                .ToList()
        };

        var cacheId = await _cacheRepo.SaveAsync(q.RunId, cachePayload, ct);
        // =======================================================

        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                // materializa série
                var raw = g.Select(x => new Point(x.Ts, x.Value)).ToList();

                // downsample (ou não)
                var downs = noDownsample ? raw : _down.MinMax(raw, maxPts);

                var points = downs
                    .Select(p => new object[] { p.Ts, p.Val })
                    .ToList();

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

        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
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