using System.Globalization;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class UnbalanceSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;

    public UnbalanceSeriesHandler(IRunContextRepository runs, IMeasurementsRepository meas, IPlotMetaBuilder meta)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
    }

    public async Task<IResult> HandleAsync(
        UnbalanceRunQuery q,
        UnbalanceRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        CancellationToken ct)
    {
        var maxPts = Math.Max(q.MaxPoints, 100);

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
                // devolve vazio p/ não quebrar front
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

            var downs = TimeBucketDownsampleMinMax(
                ratio.Select(p => (p.ts, p.ratio)),
                maxPts);

            var points = downs
                .Select(d => new object[] { d.ts, d.val * 100.0 })
                .ToList();

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

        var windowFrom = w.FromUtc ?? rows.Min(r => r.Ts);
        var windowTo = w.ToUtc ?? rows.Max(r => r.Ts);
        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        // meta (sintético)
        var meas = new MeasurementsQuery(
            Quantity: "unbalance",
            Component: "ratio",
            Unit: "%"
        );
        var plotMeta = _meta.Build(w, ctx, meas);

        return Results.Ok(new
        {
            run_id = q.RunId,
            data,
            kind,
            metric = "unbalance",
            pmu_count = series.Count,
            window = new { from = windowFrom, to = windowTo },
            meta = plotMeta,
            series
        });
    }

    private static IEnumerable<(DateTime ts, double val)> TimeBucketDownsampleMinMax(
        IEnumerable<(DateTime ts, double val)> points, int maxPoints) => points;
}
