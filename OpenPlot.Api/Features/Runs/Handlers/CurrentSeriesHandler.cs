using System.Globalization;
using System.Collections.Generic;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
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

    public CurrentSeriesHandler(IRunContextRepository runs, IMeasurementsRepository meas, IPlotMetaBuilder meta)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
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

        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                var downs = _down.MinMax(
                    g.Select(x => new Point(x.Ts, x.Value)).ToList(),
                    maxPts);

                var points = downs
                    .Select(p => new object[] { p.Ts, p.Val })
                    .ToList();

                return new
                {
                    pmu = any.IdName,
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

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);
        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        var plotMeta = _meta.Build(w, ctx, meas);

        return Results.Ok(new
        {
            modes,
            run_id = q.RunId,
            data,
            unit = "raw",
            tri,
            phase = tri ? "ABC" : uphase,
            resolved = new { pdc = ctx.PdcName, pmu_count = series.Select(s => s.pmu).Distinct().Count() },
            window = new { from = windowFrom, to = windowTo2 },
            meta = plotMeta,
            series
        });
    }
}