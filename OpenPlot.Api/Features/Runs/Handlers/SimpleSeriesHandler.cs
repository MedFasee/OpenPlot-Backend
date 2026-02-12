using System.Globalization;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

public sealed class SimpleSeriesHandler
{
    private readonly IRunContextRepository _runRepo;
    private readonly IMeasurementsRepository _measRepo;
    private readonly ITimeSeriesDownsampler _down;
    private readonly IPlotMetaBuilder _meta;

    public SimpleSeriesHandler(
        IRunContextRepository runRepo,
        IMeasurementsRepository measRepo,
        ITimeSeriesDownsampler down,
        IPlotMetaBuilder meta)
    {
        _runRepo = runRepo;
        _measRepo = measRepo;
        _down = down;
        _meta = meta;
    }

    public async Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        CancellationToken ct)
    {
        var maxPts = Math.Max(q.MaxPoints, 100);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepo.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var rows = await _measRepo.QueryAsync(ctx, meas, ct);
        if (rows.Count == 0) return Results.NotFound("Nada encontrado para esse run/filtro.");

        var plotMeta = _meta.Build(q, w, ctx, meas); // <-- 1x

        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();
                var pts = g.Select(x => new Point(x.Ts, x.Value)).ToList();
                var down = _down.MinMax(pts, maxPts);

                return new SeriesDto(
                    Pdc: any.PdcName,
                    Pmu: any.IdName,
                    SignalId: any.SignalId,
                    PdcPmuId: any.PdcPmuId,
                    Unit: meas.Quantity == "frequency" ? "Hz" : "raw",
                    Meta: null, // <-- não é meta do plot
                    Points: down.Select(p => new object[] { p.Ts, p.Val }).ToList()
                );
            })
            .ToList();

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        return Results.Ok(new SeriesResponseDto(
            RunId: q.RunId,
            Data: data,
            Resolved: new { pdc = rows.First().PdcName, pmu_count = series.Select(s => s.Pmu).Distinct().Count() },
            Window: new { from = ctx.FromUtc, to = ctx.ToUtc },
            Meta: plotMeta,     // <-- AQUI
            Series: series
        ));
    }
}
