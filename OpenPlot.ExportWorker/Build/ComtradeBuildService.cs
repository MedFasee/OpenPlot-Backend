using OpenPlot.ExportWorker.Comtrade;
using OpenPlot.ExportWorker.Domain;

namespace OpenPlot.ExportWorker.Build;

public sealed class ComtradeBuildService
{
    public List<PmuComtrade> Build(
        RunContext run,
        List<MeasurementRow> rows,
        int nominalFps,
        Func<int, string, Task>? onProgress = null)
    {
        var sampleRate = nominalFps > 0 ? nominalFps : (run.SelectRate > 0 ? run.SelectRate : 60);

        // timeline baseada na freq real do sistema (nominalFps)
        var timeline = BuildTimeline(run.FromUtc, run.ToUtc, sampleRate);
        int n = timeline.Length;

        // tolerância: metade do passo da timeline em ticks (1 tick = 100ns)
        long stepTicks = n > 1 ? (timeline[1] - timeline[0]).Ticks : TimeSpan.TicksPerSecond / sampleRate;
        long halfStepTicks = stepTicks / 2;

        // agrupa por PMU (id_name)
        var pmuGroups = rows.GroupBy(r => r.IdName).ToList();

        int total = pmuGroups.Count;
        int done = 0;

        var pmus = new List<PmuComtrade>(capacity: Math.Max(0, total));

        foreach (var group in pmuGroups)
        {
            done++;
            if (onProgress is not null)
            {
                int p = 20 + (int)Math.Round(40.0 * done / Math.Max(1, total));
                onProgress(p, $"Montando PMU {done}/{total} ({group.Key})...").GetAwaiter().GetResult();
            }

            var pmuName = group.Key;
            var pmuSafe = Naming.SafeFileBase(pmuName);

            var analogs = new List<AnalogSeries>();
            var digitals = new List<DigitalSeries>();

            int aIdx = 1;
            int dIdx = 1;

            foreach (var sigGrp in group.GroupBy(x => x.SignalId).OrderBy(g => g.Key))
            {
                var list = sigGrp.OrderBy(x => x.Ts).ToList();
                if (list.Count == 0) continue;

                var meta = list[0];

                // pontos ordenados por timestamp UTC para binary search
                var ticks = new long[list.Count];
                var vals  = new double[list.Count];
                for (int k = 0; k < list.Count; k++)
                {
                    ticks[k] = list[k].Ts.ToUniversalTime().Ticks;
                    vals[k]  = list[k].Value;
                }

                if (SignalNaming.IsDigital(meta))
                {
                    var dName = SignalNaming.MapDigitalName(meta);
                    var values = new bool[n];
                    bool last = false;
                    bool hasLast = false;

                    for (int i = 0; i < n; i++)
                    {
                        if (TryFindNearest(ticks, vals, timeline[i].Ticks, halfStepTicks, out var v))
                        {
                            last = v >= 0.5;
                            hasLast = true;
                        }
                        values[i] = hasLast ? last : false;
                    }

                    digitals.Add(new DigitalSeries(dIdx++, dName, values));
                }
                else
                {
                    var chName = SignalNaming.MapAnalogName(meta);
                    var unit   = SignalNaming.MapAnalogUnit(meta);
                    var values = new double[n];
                    double last = 0.0;
                    bool hasLast = false;

                    for (int i = 0; i < n; i++)
                    {
                        if (TryFindNearest(ticks, vals, timeline[i].Ticks, halfStepTicks, out var v))
                        {
                            last = v;
                            hasLast = true;
                        }
                        values[i] = hasLast ? last : 0.0;
                    }

                    analogs.Add(new AnalogSeries(aIdx++, chName, unit, values));
                }
            }

            if (analogs.Count == 0 && digitals.Count == 0) continue;

            pmus.Add(new PmuComtrade(
                PmuDisplayName: pmuName,
                PmuFileSafeName: pmuSafe,
                StartUtc: run.FromUtc,
                SampleRate: sampleRate,
                Analogs: analogs,
                Digitals: digitals
            ));
        }

        return pmus;
    }

    /// <summary>
    /// Busca o ponto mais próximo de <paramref name="targetTicks"/> dentro de ±<paramref name="halfStepTicks"/>.
    /// Usa binary search no array ordenado <paramref name="ticks"/>.
    /// </summary>
    private static bool TryFindNearest(long[] ticks, double[] vals, long targetTicks, long halfStepTicks, out double value)
    {
        int lo = 0, hi = ticks.Length - 1, best = -1;
        long bestDist = halfStepTicks + 1;

        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            long dist = Math.Abs(ticks[mid] - targetTicks);

            if (dist < bestDist)
            {
                bestDist = dist;
                best = mid;
            }

            if (ticks[mid] < targetTicks)
                lo = mid + 1;
            else if (ticks[mid] > targetTicks)
                hi = mid - 1;
            else
                break; // match exato
        }

        if (best >= 0 && bestDist <= halfStepTicks)
        {
            value = vals[best];
            return true;
        }

        value = 0.0;
        return false;
    }

    private static DateTimeOffset[] BuildTimeline(DateTimeOffset fromUtc, DateTimeOffset toUtc, int rate)
    {
        long stepUs = (long)Math.Round(1_000_000.0 / rate);

        var durUs = (long)Math.Max(0, (toUtc - fromUtc).TotalMilliseconds * 1000.0);
        long count = durUs / stepUs + 1;

        if (count > int.MaxValue)
            throw new InvalidOperationException("Janela grande demais para timeline.");

        var arr = new DateTimeOffset[count];
        for (int i = 0; i < (int)count; i++)
            arr[i] = fromUtc.AddTicks((stepUs * i) * 10); // 10 ticks = 1µs

        return arr;
    }
}