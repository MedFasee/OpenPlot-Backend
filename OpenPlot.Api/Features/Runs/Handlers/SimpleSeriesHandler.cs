using System.Globalization;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Features.Ui;

public sealed class SimpleSeriesHandler
{
    private readonly IRunContextRepository _runRepo;
    private readonly IMeasurementsRepository _measRepo;
    private readonly ITimeSeriesDownsampler _down;
    private readonly IPlotMetaBuilder _meta;
    private readonly IAnalysisCacheRepository _cacheRepo;

    public SimpleSeriesHandler(
        IRunContextRepository runRepo,
        IMeasurementsRepository measRepo,
        ITimeSeriesDownsampler down,
        IPlotMetaBuilder meta,
        IAnalysisCacheRepository cacheRepo)
    {
        _runRepo = runRepo;
        _measRepo = measRepo;
        _down = down;
        _meta = meta;
        _cacheRepo = cacheRepo;
    }

    public Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        CancellationToken ct)
        => HandleAsync(q, w, meas, modes: null, ct);

    public async Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        Dictionary<string, object?>? modes,
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

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);

        var cachePayload = new RowsCacheV2
        {
            From = windowFrom.ToUniversalTime(),
            To = windowTo2.ToUniversalTime(),
            SelectRate = (int)ctx.SelectRate, // ajuste se necessário
            Series = rows
        .GroupBy(r => r.SignalId)
        .Select(g =>
        {
            var first = g.First();

            return new RowsCacheSeries
            {
                SignalId = first.SignalId,
                PdcPmuId = first.PdcPmuId,
                IdName = first.IdName,
                PdcName = first.PdcName,
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

        var dataStr = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        return Results.Ok(new
        {
            modes,

            run_id = q.RunId,
            data = dataStr,

            cache_id = cacheId,

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