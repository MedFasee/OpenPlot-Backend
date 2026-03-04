using System.Globalization;
using System.Numerics;
using System.Text.Json;
using MathNet.Numerics.IntegralTransforms;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Features.PostProcessing.Handlers;

public static class Dft
{
    public sealed record SpecPoint(double Hz, double Mag);
    public sealed record Spec(double Sr, int N, double FMin, IReadOnlyList<SpecPoint> Points);

    // Igual MedPlot: fMin = 2*sr/N
    public static double FMin(double sr, int n) => (2.0 * sr) / n;

    // FFT single-sided (Magnitudes)
    public static Spec ForwardSingleSided(double[] y, double sr)
    {
        var n = y.Length;
        if (n < 2) return new Spec(sr, n, 0, Array.Empty<SpecPoint>());

        var aux = new Complex[n];
        for (int i = 0; i < n; i++)
            aux[i] = new Complex(y[i], 0);

        Fourier.Forward(aux, FourierOptions.Matlab);

        var half = n / 2 + 1;
        var pts = new SpecPoint[half];

        for (int k = 0; k < half; k++)
        {
            var hz = (k * sr) / n;
            var mag = (k == 0) ? 0.0 : 2.0 * aux[k].Magnitude; // MedPlot
            pts[k] = new SpecPoint(hz, mag);
        }

        return new Spec(sr, n, FMin(sr, n), pts);
    }

    // Resample “hold-last” num grid uniforme
    public static double[] ResampleHoldLast(IReadOnlyList<Point> raw, double sr, int? nMax = null)
    {
        if (raw.Count == 0) return Array.Empty<double>();

        var pts = raw.OrderBy(p => p.Ts).ToList();

        if (nMax is > 0 && pts.Count > nMax.Value)
            pts = pts.Skip(pts.Count - nMax.Value).ToList();

        var dtTicks = (long)Math.Round(TimeSpan.TicksPerSecond / sr);
        if (dtTicks <= 0) dtTicks = 1;

        var t0 = pts[0].Ts;
        var tN = pts[^1].Ts;
        var spanTicks = (tN - t0).Ticks;

        var n = (int)(spanTicks / dtTicks) + 1;
        if (n < 2) n = 2;

        var y = new double[n];

        int j = 0;
        double last = pts[0].Val;

        for (int i = 0; i < n; i++)
        {
            var ti = t0.AddTicks(i * dtTicks);

            while (j < pts.Count && pts[j].Ts <= ti)
            {
                last = pts[j].Val;
                j++;
            }

            y[i] = last;
        }

        return y;
    }

    public static DftComputeResult Compute(RowsCacheV2 payload)
    {
        if (payload is null)
            throw new ArgumentNullException(nameof(payload));

        if (payload.Series is null || payload.Series.Count == 0)
            throw new InvalidOperationException("Nenhuma série encontrada no cache.");

        if (payload.SelectRate <= 0)
            throw new InvalidOperationException("SelectRate inválido.");

        var sr = (double)payload.SelectRate;

        var orderedSeries = payload.Series
            .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var specs = new Dictionary<string, Spec>(StringComparer.OrdinalIgnoreCase);

        foreach (var serie in orderedSeries)
        {
            var raw = serie.Points
                .Select(p => new Point(p.Ts, p.Value))
                .ToList();

            if (raw.Count < 2)
                continue;

            var y = ResampleHoldLast(raw, sr);
            var spec = ForwardSingleSided(y, sr);

            specs[BuildSeriesName(serie)] = spec;
        }

        if (specs.Count == 0)
            throw new InvalidOperationException("Nenhuma série válida para DFT.");

        return new DftComputeResult
        {
            Specs = specs
        };
    }

    private static string BuildSeriesName(RowsCacheSeries s)
    {
        var hasPhase = !string.IsNullOrWhiteSpace(s.Phase);
        var hasComponent = !string.IsNullOrWhiteSpace(s.Component);

        if (hasPhase && hasComponent)
            return $"{s.IdName} - {s.Phase} - {s.Component}";

        if (hasPhase)
            return $"{s.IdName} - {s.Phase}";

        if (hasComponent)
            return $"{s.IdName} - {s.Component}";

        return s.IdName;
    }

    private static DateTime ReadDateTime(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.String => DateTime.Parse(
                el.GetString()!,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind | DateTimeStyles.AssumeUniversal
            ).ToUniversalTime(),

            _ => throw new InvalidOperationException($"Timestamp inválido no cache: {el.ValueKind}")
        };
    }

    private static double ReadDouble(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Number => el.GetDouble(),
            JsonValueKind.String => double.Parse(el.GetString()!, CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException($"Valor numérico inválido no cache: {el.ValueKind}")
        };
    }
}

public sealed class DftComputeResult
{
    public Dictionary<string, Dft.Spec> Specs { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}