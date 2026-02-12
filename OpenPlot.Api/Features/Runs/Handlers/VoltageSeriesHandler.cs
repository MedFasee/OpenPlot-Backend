using System.Globalization;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class VoltageSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;

    public VoltageSeriesHandler(IRunContextRepository runs, IMeasurementsRepository meas, IPlotMetaBuilder meta)
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

        var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "pu"))
            return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

        var maxPts = Math.Max(q.MaxPoints, 100);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runs.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        // MeasurementsQuery "equivalente" (serve tanto pro repo quanto pro meta)
        var measQuery = new MeasurementsQuery(
            Quantity: "voltage",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri && !string.IsNullOrWhiteSpace(pmuName) ? new[] { pmuName } : null,
            Unit: unit
        );

        var rows = await _meas.QueryPhasorAsync(ctx, measQuery, ct);

        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var plotMeta = _meta.Build(w, ctx, measQuery);

        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                var downs = TimeBucketDownsampleMinMax(
                    g.Select(x => (x.Ts, x.Value)),
                    maxPts);

                var typed = unit == "pu"
                    ? PerUnit.ToVoltagePu(downs, any.VoltLevel)
                    : downs.ToList();

                var points = typed
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
                        phase = (any.Phase ?? "").Trim().ToUpperInvariant(),   // <- fase real (A/B/C)
                        component = (any.Component ?? "").Trim().ToUpperInvariant(),
                        volt_level_kV = any.VoltLevel is null ? (double?)null : any.VoltLevel.Value / 1000.0
                    },
                    points
                };
            })
            .ToList();

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo = toUtc ?? rows.Max(r => r.Ts);
        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        return Results.Ok(new
        {
            run_id = q.RunId,
            data,
            unit,
            tri,
            phase = tri ? "ABC" : uphase, // pedido do usuário (ok)
            resolved = new
            {
                pdc = ctx.PdcName,
                pmu_count = series.Select(s => s.pmu).Distinct().Count()
            },
            window = new { from = windowFrom, to = windowTo },
            meta = plotMeta,
            series
        });
    }

    private static IEnumerable<(DateTime ts, double val)> TimeBucketDownsampleMinMax(
        IEnumerable<(DateTime ts, double val)> points, int maxPoints) => points;
}
