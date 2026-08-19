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

public sealed class UnbalanceSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IUiMenuService _uiMenus;
    private readonly ILogger<UnbalanceSeriesHandler> _logger;

    public UnbalanceSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepo,
        IUiMenuService uiMenus,
        ILogger<UnbalanceSeriesHandler> logger)
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
        UnbalanceRunQuery q,
        UnbalanceRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var ctx = await _runs.ResolveAsync(q.RunId, w.FromUtc, w.ToUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var kind = req.Kind == SeqKind.Current ? "current" : "voltage";

        var frontWatch = Stopwatch.StartNew();
        _logger.LogInformation("[BYRUN][UNBALANCE][FRONT][START] runId={RunId} kind={Kind} maxPoints={MaxPoints}", q.RunId, kind, noDownsample ? "all" : maxPts);

        var rows = await _meas.QueryAbcMagAngAsync(
            ctx,
            kind,
            pmuList.Count == 0 ? null : pmuList,
            w.FromUtc,
            w.ToUtc,
            ct,
            noDownsample ? null : maxPts);

        frontWatch.Stop();
        _logger.LogInformation("[BYRUN][UNBALANCE][FRONT][END] runId={RunId} kind={Kind} elapsedMs={ElapsedMs} rows={Rows}", q.RunId, kind, frontWatch.ElapsedMilliseconds, rows.Count);

        if (rows.Count == 0)
            return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

        var projection = BuildUnbalanceProjection(rows, kind, noDownsample, maxPts);
        var series = projection.Series;
        var windowFrom = w.FromUtc ?? projection.WindowFrom;
        var windowTo = w.ToUtc ?? projection.WindowTo;
        var cacheId = Guid.NewGuid();

        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;
            _logger.LogInformation("[BYRUN][UNBALANCE][CACHE-BG][START] runId={RunId} kind={Kind} cacheId={CacheId}", q.RunId, kind, cacheId);

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

                var fullProjection = BuildUnbalanceProjection(fullRows, kind, true, int.MaxValue);
                var cacheSeriesFull = fullProjection.CachePoints
                    .GroupBy(x => x.pmuId)
                    .Select(g => _seriesAssembly.BuildCacheSeries(
                        signalId: 0,
                        pdcPmuId: 0,
                        idName: g.Key,
                        pdcName: ctx.PdcName,
                        referenceTerminal: null,
                        unit: "%",
                        phase: null,
                        quantity: kind,
                        component: "ratio",
                        points: g.Select(x => (x.ts, x.value))))
                    .ToList();

                var cachePayloadFull = _seriesAssembly.BuildCachePayload(
                    w.FromUtc ?? fullProjection.WindowFrom,
                    w.ToUtc ?? fullProjection.WindowTo,
                    ctx.SelectRate ?? 0,
                    cacheSeriesFull);

                await _cacheRepo.SaveAsync(cacheId, q.RunId, cachePayloadFull, CancellationToken.None);
                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao persistir cache assíncrono de unbalance/by-run. runId={RunId}", q.RunId);
            }
            finally
            {
                bgWatch.Stop();
                _logger.LogInformation("[BYRUN][UNBALANCE][CACHE-BG][END] runId={RunId} kind={Kind} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}", q.RunId, kind, cacheId, bgWatch.ElapsedMilliseconds, fullRowsCount, persisted);
            }
        });

        var meas = new MeasurementsQuery(
            Quantity: kind,
            Component: "ratio",
            Unit: "%"
        );
        var plotMeta = _meta.Build(w, ctx, meas);
        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(
                WindowFromUtc: windowFrom,
                WindowToUtc: windowTo,
                SelectRate: ctx.SelectRate,
                TotalSeriesCount: series.Count,
                ValidSeriesCount: series.Count,
                Quantity: kind,
                Component: "ratio"));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo, series, plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = "percent",
                ["kind"] = kind,
                ["metric"] = "unbalance"
            })
            .Build();

        return Results.Ok(response);
    }

    private UnbalanceProjection BuildUnbalanceProjection(
        IReadOnlyList<PhasorAbcRow> rows,
        string kind,
        bool noDownsample,
        int maxPts)
    {
        static List<(DateTime ts, double ratio)> RatioPointwise(
            List<(DateTime ts, double mag)> neg,
            List<(DateTime ts, double mag)> pos,
            TimeSpan tolerance)
        {
            var outp = new List<(DateTime ts, double ratio)>();
            int i = 0, j = 0;

            while (i < neg.Count && j < pos.Count)
            {
                var tn = neg[i].ts;
                var tp = pos[j].ts;
                var t = tn > tp ? tn : tp;

                while (i < neg.Count && neg[i].ts < t && (t - neg[i].ts) > tolerance) i++;
                while (j < pos.Count && pos[j].ts < t && (t - pos[j].ts) > tolerance) j++;

                if (i >= neg.Count || j >= pos.Count) break;

                tn = neg[i].ts;
                tp = pos[j].ts;

                if (Math.Abs((tn - t).TotalMilliseconds) > tolerance.TotalMilliseconds ||
                    Math.Abs((tp - t).TotalMilliseconds) > tolerance.TotalMilliseconds)
                {
                    var minT = tn < tp ? tn : tp;
                    if (minT == tn) i++; else j++;
                    continue;
                }

                var den = pos[j].mag;
                if (den > 0)
                    outp.Add((t, neg[i].mag / den));

                i++; j++;
            }

            return outp;
        }

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
            {
                var f0 = sigRows.First();
                series.Add(new
                {
                    pmu = f0.IdName,
                    pdc = f0.PdcName,
                    unit = "percent",
                    meta = new { kind, metric = "unbalance" },
                    points = Array.Empty<object>()
                });
                continue;
            }

            vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
            vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
            vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
            vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
            vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
            vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

            var seqPos = Sequences.ComputeSequenceMagnitudeMedPlot(vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, "pos");
            var seqNeg = Sequences.ComputeSequenceMagnitudeMedPlot(vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, "neg");

            var first = sigRows.First();

            if (seqPos.Count == 0 || seqNeg.Count == 0)
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new { kind, metric = "unbalance" },
                    points = Array.Empty<object>()
                });
                continue;
            }

            const double EPS = 1e-12;
            if (!seqPos.Any(p => Math.Abs(p.mag) > EPS))
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new { kind, metric = "unbalance" },
                    points = Array.Empty<object>()
                });
                continue;
            }

            var ratio = RatioPointwise(
                seqNeg.Select(p => (p.ts, p.mag)).ToList(),
                seqPos.Select(p => (p.ts, p.mag)).ToList(),
                TimeSpan.FromMilliseconds(3));

            if (ratio.Count == 0)
            {
                series.Add(new
                {
                    pmu = first.IdName,
                    pdc = first.PdcName,
                    unit = "percent",
                    meta = new { kind, metric = "unbalance" },
                    points = Array.Empty<object>()
                });
                continue;
            }

            var ratioPercent = ratio.Select(r => (r.ts, value: r.ratio * 100.0)).ToList();
            foreach (var point in ratioPercent)
            {
                cachePoints.Add((first.IdName, point.ts, point.value));
            }

            var points = _seriesAssembly.BuildPoints(
                ratio.Select(p => (p.ts, p.ratio)),
                true,
                maxPts,
                _down,
                outputScale: 100.0);

            series.Add(new
            {
                pmu = first.IdName,
                pdc = first.PdcName,
                unit = "percent",
                meta = new
                {
                    kind,
                    metric = "unbalance",
                    volt_level_kV = first.VoltLevel is null ? (double?)null : first.VoltLevel.Value / 1000.0
                },
                points
            });
        }

        return new UnbalanceProjection(
            series,
            cachePoints,
            rows.Min(r => r.Ts),
            rows.Max(r => r.Ts));
    }

    private sealed record UnbalanceProjection(
        List<object> Series,
        List<(string pmuId, DateTime ts, double value)> CachePoints,
        DateTime WindowFrom,
        DateTime WindowTo);
}
