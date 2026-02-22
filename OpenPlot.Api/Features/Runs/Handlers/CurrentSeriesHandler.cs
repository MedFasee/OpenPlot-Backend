using System.Globalization;
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

    // Mantém compatibilidade (chamadas antigas)
    public Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, CancellationToken ct)
        => HandleAsync(q, w, ui: null, ct);

    // NOVO: recebe UI (já resolvida no endpoint)
    public async Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, UiCatalog? ui, CancellationToken ct)
    {
        var tri = q.Tri;
        var pmuName = q.Pmu?.Trim();
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

        var pmuNames = q.Pmus?
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
                ? (string.IsNullOrWhiteSpace(pmuName) ? null : new[] { pmuName })
                : (pmuNames is { Length: > 0 } ? pmuNames : null),
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
            ui, 
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