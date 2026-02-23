using OpenPlot.ExportWorker.Comtrade;
using OpenPlot.ExportWorker.Domain;

namespace OpenPlot.ExportWorker.Build;

public sealed class ComtradeBuildService
{
    public List<PmuComtrade> Build(
        RunContext run,
        List<MeasurementRow> rows,
        Func<int, string, Task>? onProgress = null)
    {
        var sampleRate = run.SelectRate > 0 ? run.SelectRate : 60;

        // timeline ideal baseada na janela do run e na taxa
        var timeline = BuildTimeline(run.FromUtc, run.ToUtc, sampleRate);
        int n = timeline.Length;

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

                // map Ts->Value (UTC)
                var map = new Dictionary<DateTimeOffset, double>(list.Count);
                foreach (var p in list)
                    map[p.Ts.ToUniversalTime()] = p.Value;

                if (SignalNaming.IsDigital(meta))
                {
                    var dName = SignalNaming.MapDigitalName(meta);

                    var values = new bool[n];
                    bool last = false;
                    bool hasLast = false;

                    for (int i = 0; i < n; i++)
                    {
                        if (map.TryGetValue(timeline[i], out var v))
                        {
                            // regra de binarização: >= 0.5 => 1
                            last = v >= 0.5;
                            hasLast = true;
                            values[i] = last;
                        }
                        else
                        {
                            values[i] = hasLast ? last : false;
                        }
                    }

                    digitals.Add(new DigitalSeries(dIdx++, dName, values));
                }
                else
                {
                    var chName = SignalNaming.MapAnalogName(meta);
                    var unit = SignalNaming.MapAnalogUnit(meta);

                    var values = new double[n];
                    double last = 0.0;
                    bool hasLast = false;

                    for (int i = 0; i < n; i++)
                    {
                        if (map.TryGetValue(timeline[i], out var v))
                        {
                            values[i] = v;
                            last = v;
                            hasLast = true;
                        }
                        else
                        {
                            values[i] = hasLast ? last : 0.0;
                        }
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

    private static DateTimeOffset[] BuildTimeline(DateTimeOffset fromUtc, DateTimeOffset toUtc, int rate)
    {
        long stepUs = (long)Math.Round(1_000_000.0 / rate);

        var durUs = (long)Math.Max(0, (toUtc - fromUtc).TotalMilliseconds * 1000.0);
        long count = durUs / stepUs + 1;

        if (count > int.MaxValue)
            throw new InvalidOperationException("Janela grande demais para timeline.");

        var arr = new DateTimeOffset[count];
        for (int i = 0; i < (int)count; i++)
            arr[i] = fromUtc.AddTicks((stepUs * i) * 10); // 10 ticks = 1us

        return arr;
    }
}