using System.Globalization;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class CurrentSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;

    public CurrentSeriesHandler(IRunContextRepository runs, IMeasurementsRepository meas, IPlotMetaBuilder meta)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
    }

    public async Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, CancellationToken ct)
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

        var maxPts = Math.Max(q.MaxPoints, 100);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runs.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var meas = new MeasurementsQuery(
            Quantity: "current",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri && !string.IsNullOrWhiteSpace(pmuName) ? new[] { pmuName } : null,
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

                var downs = TimeBucketDownsampleMinMax(
                    g.Select(x => (x.Ts, x.Value)),
                    maxPts);

                var points = downs
                    .Select(p => new object[] { p.ts, p.val })
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

        Console.WriteLine($"[HANDLER] meas = {meas.GetType().FullName} qty='{meas.Quantity}' comp='{meas.Component}' unit='{meas.Unit}'");


        var plotMeta = _meta.Build(w, ctx, meas);

        return Results.Ok(new
        {
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

    private static IEnumerable<(DateTime ts, double val)> TimeBucketDownsampleMinMax(
        IEnumerable<(DateTime ts, double val)> points, int maxPoints) => points;
}
