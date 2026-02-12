using System.Globalization;
using System.Net;
using MathNet.Numerics.Differentiation;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class SeqSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;

    public SeqSeriesHandler(IRunContextRepository runs, IMeasurementsRepository meas, IPlotMetaBuilder meta)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
    }

    public async Task<IResult> HandleAsync(
        SeqRunQuery q,
        SeqRequest req,
        WindowQuery w,
        IReadOnlyList<string> pmuList,
        CancellationToken ct)
    {
        var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "pu"))
            return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

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

        string seqNorm = req.Seq switch
        {
            SeqType.Pos => "pos",
            SeqType.Neg => "neg",
            _ => "zero"
        };

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
                baseValue = 1.0; // MedPlot
            }

            double Unitize(double m) => unit == "pu" ? (m / baseValue) : m;

            var downs = TimeBucketDownsampleMinMax(
                seqSeries.Select(p => (p.ts, Unitize(p.mag))),
                maxPts);

            var points = downs.Select(d => new object[] { d.ts, d.val }).ToList();

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
        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        // meta (sintético) -> precisa refletir a grandeza real (current/voltage)
        var meas = new MeasurementsQuery(
            Quantity: kind,                // "current" ou "voltage"
            Component: "mag",              // sequência é magnitude (o ponto é |I1|/|V1|)
            PhaseMode: PhaseMode.Any,      // não é ABC "cru", é série já processada
            Phase: null,
            PmuNames: pmuList.Count == 0 ? null : pmuList,
            Unit: unit                     // "raw" ou "pu" (não "A"/"V")
        );

        // se você quiser que o título mostre que é sequência, passe isso via ctx/req no builder
        // ou adicione um campo opcional "Seq" no MeasurementsQuery no futuro.
        // Por agora, pelo menos o yLabel fica correto.
        var plotMeta = _meta.Build(w, ctx, meas);

        return Results.Ok(new
        {
            run_id = q.RunId,
            data,
            kind,
            seq = seqNorm,
            unit,
            pmu_count = series.Count,
            window = new { from = windowFrom, to = windowTo },
            meta = plotMeta,
            series
        });
    }

    private static IEnumerable<(DateTime ts, double val)> TimeBucketDownsampleMinMax(
        IEnumerable<(DateTime ts, double val)> points, int maxPoints) => points;
}
