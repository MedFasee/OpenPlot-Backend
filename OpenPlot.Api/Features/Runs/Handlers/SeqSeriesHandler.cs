using System.Globalization;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Features.Ui;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class SeqSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;

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
        IAnalysisCacheRepository cacheRepo)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _cacheRepo = cacheRepo;
    }

    // Mantém compatibilidade (chamadas antigas)
    public Task<IResult> HandleAsync(
        SeqRunQuery q,
        SeqRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        CancellationToken ct)
        => HandleAsync(q, req, w, pmuList, modes: null, ct);

    // NOVO: recebe UI (já resolvida no endpoint)
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

        var rows = await _meas.QueryAbcMagAngAsync(
            ctx,
            kind,
            pmuList.Count == 0 ? null : pmuList,
            w.FromUtc,
            w.ToUtc,
            ct);

        if (rows.Count == 0)
            return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

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

            // Armazena dados processados para cache
            var processedSeq = seqSeries.Select(p => (p.ts, value: Unitize(p.mag))).ToList();
            foreach (var point in processedSeq)
            {
                cachePoints.Add((first.IdName, point.ts, point.value));
            }

            var raw = seqSeries
                .Select(p => new Point(p.ts, Unitize(p.mag)))
                .ToList();

            var downs = noDownsample ? raw : _down.MinMax(raw, maxPts);

            var points = downs
                .Select(p => new object[] { p.Ts, p.Val })
                .ToList();

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

        if (series.Count == 0)
            return Results.BadRequest("Nenhuma PMU pôde ser processada.");

        var windowFrom = w.FromUtc ?? rows.Min(r => r.Ts);
        var windowTo = w.ToUtc ?? rows.Max(r => r.Ts);

        // ===== CACHE =====
        var cachePayload = new RowsCacheV2
        {
            From = windowFrom.ToUniversalTime(),
            To = windowTo.ToUniversalTime(),
            SelectRate = (int)ctx.SelectRate,
            Series = cachePoints
                .GroupBy(x => x.pmuId)
                .Select(g =>
                {
                    return new RowsCacheSeries
                    {
                        SignalId = 0, // Sequências não têm signal_id
                        PdcPmuId = 0,
                        IdName = g.Key,
                        PdcName = ctx.PdcName,
                        Unit = unit,
                        Phase = seqNorm,
                        Quantity = kind,
                        Component = "seq",
                        Points = g
                            .OrderBy(x => x.ts)
                            .Select(x => new RowsCachePoint
                            {
                                Ts = x.ts.ToUniversalTime(),
                                Value = x.value
                            })
                            .ToList()
                    };
                })
                .ToList()
        };

        var cacheId = await _cacheRepo.SaveAsync(q.RunId, cachePayload, ct);
        // =======================================================

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

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo, series, plotMeta)
            .WithModes(modes)
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
}