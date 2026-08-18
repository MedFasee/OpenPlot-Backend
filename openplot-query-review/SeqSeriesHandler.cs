using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class SeqSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IUiMenuService _uiMenus;
    private readonly ILogger<SeqSeriesHandler> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="SeqSeriesHandler"/> class.
    /// </summary>
    /// <param name="runs">The runs repository.</param>
    /// <param name="meas">The measurements repository.</param>
    /// <param name="meta">The metadata builder.</param>
    /// <param name="cacheRepo">The analysis cache repository.</param>
    public SeqSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepo,
        IUiMenuService uiMenus,
        ILogger<SeqSeriesHandler> logger)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _seriesAssembly = seriesAssembly;
        _cacheRepo = cacheRepo;
        _uiMenus = uiMenus;
        _logger = logger;
    }

    // Recebe UI já resolvida no endpoint
    public async Task<IResult> HandleAsync(
        SeqRunQuery q,
        SeqRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "pu"))
            return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var ctx = await _runs.ResolveAsync(q.RunId, w.FromUtc, w.ToUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var kind = req.Kind == SeqKind.Current ? "current" : "voltage";

        var frontWatch = Stopwatch.StartNew();
        _logger.LogInformation("[BYRUN][SEQ][FRONT][START] runId={RunId} kind={Kind} maxPoints={MaxPoints}", q.RunId, kind, noDownsample ? "all" : maxPts);

        var frontRows = await _meas.QueryAbcMagAngAsync(
            ctx,
            kind,
            pmuList.Count == 0 ? null : pmuList,
            w.FromUtc,
            w.ToUtc,
            ct,
            noDownsample ? null : maxPts);

        frontWatch.Stop();
        _logger.LogInformation("[BYRUN][SEQ][FRONT][END] runId={RunId} kind={Kind} elapsedMs={ElapsedMs} rows={Rows}", q.RunId, kind, frontWatch.ElapsedMilliseconds, frontRows.Count);

        if (frontRows.Count == 0)
            return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

        var cacheId = Guid.NewGuid();

        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;
            _logger.LogInformation("[BYRUN][SEQ][CACHE-BG][START] runId={RunId} kind={Kind} cacheId={CacheId}", q.RunId, kind, cacheId);

            try
            {
                var fullRows = await _meas.QueryAbcMagAngAsync(
                    ctx,
                    kind,
                    pmuList.Count == 0 ? null : pmuList,
                    w.FromUtc,
                    w.ToUtc,
                    CancellationToken.None,
                    null);

                fullRowsCount = fullRows.Count;

                if (fullRows.Count == 0)
                    return;

                var fullSeries = BuildSequenceSeries(fullRows, req, q, kind, out var fullWindowFrom, out var fullWindowTo, out var fullSeqNorm);
                if (fullSeries.CachePoints.Count == 0)
                    return;

                var cacheSeriesFull = fullSeries.CachePoints
                    .GroupBy(x => x.pmuId)
                    .Select(g => _seriesAssembly.BuildCacheSeries(
                        signalId: 0,
                        pdcPmuId: 0,
                        idName: g.Key,
                        pdcName: ctx.PdcName,
                        referenceTerminal: null,
                        unit: unit,
                        phase: fullSeqNorm,
                        quantity: kind,
                        component: "seq",
                        points: g.Select(x => (x.ts, x.value))))
                    .ToList();

                var cachePayloadFull = _seriesAssembly.BuildCachePayload(
                    fullWindowFrom,
                    fullWindowTo,
                    ctx.SelectRate ?? 0,
                    cacheSeriesFull);

                await _cacheRepo.SaveAsync(cacheId, q.RunId, cachePayloadFull, CancellationToken.None);
                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao persistir cache assíncrono de seq/by-run. runId={RunId}", q.RunId);
            }
            finally
            {
                bgWatch.Stop();
                _logger.LogInformation("[BYRUN][SEQ][CACHE-BG][END] runId={RunId} kind={Kind} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}", q.RunId, kind, cacheId, bgWatch.ElapsedMilliseconds, fullRowsCount, persisted);
            }
        });

        var rows = frontRows;

        var projection = BuildSequenceSeries(rows, req, q, kind, noDownsample, maxPts, unit);
        if (projection.Series.Count == 0)
            return Results.BadRequest("Nenhuma PMU pôde ser processada.");

        var seqNorm = projection.SeqNorm;
        var series = projection.Series;
        var windowFrom = projection.WindowFrom;
        var windowTo = projection.WindowTo;

        var pmusForMeta = pmuList.Count == 0 ? null : pmuList;

        var seqMode = req.Seq switch
        {
            SeqType.Pos => PhaseMode.SeqPos,
            SeqType.Neg => PhaseMode.SeqNeg,
            _ => PhaseMode.SeqZero
        };

        var meas = new MeasurementsQuery(
            Quantity: kind,
            Component: "mag",
            PhaseMode: seqMode,
            PmuNames: pmusForMeta,
            Unit: unit
        );

        var plotMeta = _meta.Build(w, ctx, meas);
        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(windowFrom, windowTo, ctx.SelectRate));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo, series, plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unit,
                ["kind"] = kind,
                ["seq"] = seqNorm
            })
            .Build();

        return Results.Ok(response);
    }

    private SequenceProjection BuildSequenceSeries(
        IReadOnlyList<PhasorAbcRow> rows,
        SeqRequest req,
        SeqRunQuery q,
        string kind,
        bool noDownsample,
        int maxPts,
        string unit)
    {
        string seqNorm = req.Seq switch
        {
            SeqType.Pos => "pos",
            SeqType.Neg => "neg",
            _ => "zero"
        };

        var series = new List<object>();
        var cachePoints = new List<(string pmuId, DateTime ts, double value)>();

        foreach (var g in rows.GroupBy(r => r.IdName, StringComparer.OrdinalIgnoreCase))
        {
            var sigRows = g.ToList();

            var vaMod = new List<(DateTime ts, double mag)>();
            var vbMod = new List<(DateTime ts, double mag)>();
            var vcMod = new List<(DateTime ts, double mag)>();
            var vaAng = new List<(DateTime ts, double angDeg)>();
            var vbAng = new List<(DateTime ts, double angDeg)>();
            var vcAng = new List<(DateTime ts, double angDeg)>();

            foreach (var r in sigRows)
            {
                var ph = (r.Phase ?? "").Trim().ToUpperInvariant();
                var cp = (r.Component ?? "").Trim().ToUpperInvariant();

                if (ph == "A" && cp == "MAG") vaMod.Add((r.Ts, r.Value));
                else if (ph == "B" && cp == "MAG") vbMod.Add((r.Ts, r.Value));
                else if (ph == "C" && cp == "MAG") vcMod.Add((r.Ts, r.Value));
                else if (ph == "A" && cp == "ANG") vaAng.Add((r.Ts, r.Value));
                else if (ph == "B" && cp == "ANG") vbAng.Add((r.Ts, r.Value));
                else if (ph == "C" && cp == "ANG") vcAng.Add((r.Ts, r.Value));
            }

            if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
                vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
                continue;

            vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
            vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
            vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
            vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
            vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
            vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

            var seqSeries = Sequences.ComputeSequenceMagnitudeMedPlot(
                vaMod, vbMod, vcMod,
                vaAng, vbAng, vcAng,
                seqNorm);

            if (seqSeries.Count == 0) continue;

            var first = sigRows.First();
            double baseValue = 1.0;

            if (unit == "pu" && kind == "voltage")
            {
                var lvl = q.VoltLevel ?? first.VoltLevel ?? 0;
                if (lvl > 0) baseValue = lvl / Math.Sqrt(3.0);
            }
            else if (unit == "pu" && kind == "current")
            {
                baseValue = 1.0;
            }

            double Unitize(double m) => unit == "pu" ? (m / baseValue) : m;

            var processedSeq = seqSeries.Select(p => (p.ts, value: Unitize(p.mag))).ToList();
            foreach (var point in processedSeq)
            {
                cachePoints.Add((first.IdName, point.ts, point.value));
            }

            var points = _seriesAssembly.BuildPoints(
                seqSeries.Select(p => (p.ts, Unitize(p.mag))),
                true,
                maxPts,
                _down);

            series.Add(new
            {
                pmu = first.IdName,
                pdc = first.PdcName,
                unit,
                meta = new
                {
                    kind,
                    seq = seqNorm,
                    volt_level_kV = first.VoltLevel is null ? (double?)null : first.VoltLevel.Value / 1000.0
                },
                points
            });
        }

        var windowFrom = rows.Min(r => r.Ts);
        var windowTo = rows.Max(r => r.Ts);

        return new SequenceProjection(series, cachePoints, windowFrom, windowTo, seqNorm);
    }

    private SequenceProjection BuildSequenceSeries(
        IReadOnlyList<PhasorAbcRow> rows,
        SeqRequest req,
        SeqRunQuery q,
        string kind,
        out DateTime windowFrom,
        out DateTime windowTo,
        out string seqNorm)
    {
        var projection = BuildSequenceSeries(rows, req, q, kind, true, int.MaxValue, q.Unit?.Trim().ToLowerInvariant() is "pu" ? "pu" : "raw");
        windowFrom = projection.WindowFrom;
        windowTo = projection.WindowTo;
        seqNorm = projection.SeqNorm;
        return projection;
    }

    private sealed record SequenceProjection(
        List<object> Series,
        List<(string pmuId, DateTime ts, double value)> CachePoints,
        DateTime WindowFrom,
        DateTime WindowTo,
        string SeqNorm);
}
