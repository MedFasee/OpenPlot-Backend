using System.Globalization;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Ui;

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

    public Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        CancellationToken ct)
        => HandleAsync(q, w, meas, ui: null, ct);

    public async Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        UiCatalog? ui,
        CancellationToken ct)
    {
        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepo.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var rows = await _measRepo.QueryAsync(ctx, meas, ct);
        if (rows.Count == 0) return Results.NotFound("Nada encontrado para esse run/filtro.");

        var plotMeta = _meta.Build(w, ctx, meas);

        var series = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();
                var pts = g.Select(x => new Point(x.Ts, x.Value)).ToList();
                var down = noDownsample ? pts : _down.MinMax(pts, maxPts);

                return new SeriesDto(
                    Pdc: any.PdcName,
                    Pmu: any.IdName,
                    SignalId: any.SignalId,
                    PdcPmuId: any.PdcPmuId,
                    Unit: meas.Unit ?? "raw",
                    Meta: null,
                    Points: down.Select(p => new object[] { p.Ts, p.Val }).ToList()
                );
            })
            .ToList();

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);
        var dataStr = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        return Results.Ok(new
        {
            ui,

            run_id = q.RunId,
            data = dataStr,

            resolved = new
            {
                pdc = rows.First().PdcName,
                pmu_count = series.Select(s => s.Pmu).Distinct().Count()
            },

            window = new { from = windowFrom, to = windowTo2 },

            meta = plotMeta,
            series
        });
    }
}