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

    public sealed record ZoomBounds(double Position, double Size);

    // Spec agora carrega metadados da série (pmu/phase/component/quantity)
    public sealed record Spec(double Sr, int N, double FMin, IReadOnlyList<SpecPoint> Points)
    {
        public string? Pmu { get; init; }
        public string? Phase { get; init; }
        public string? Component { get; init; }
        public string? Quantity { get; init; }
        public string? Unit { get; init; }
    }

    // Igual MedPlot: fMin = 2*sr/N
    public static double FMin(double sr, int n) => (2.0 * sr) / n;

    /// <summary>
    /// Calculates zoom bounds (Position and Size) based on sampling rate (sr) and number of data points (Ndat).
    /// Formula: fMin = 2 / (Ndat / sr)
    /// </summary>
    /// <param name="ndat">Number of data points (samples)</param>
    /// <param name="sr">Sampling rate in Hz</param>
    /// <returns>ZoomBounds with Position and Size for chart initial view</returns>
    public static ZoomBounds CalculateZoomBounds(int ndat, double sr)
    {
        if (ndat <= 0)
            throw new ArgumentException("Ndat deve ser maior que 0", nameof(ndat));
        if (sr <= 0)
            throw new ArgumentException("Sr deve ser maior que 0", nameof(sr));

        var fMin = 2.0 / ((double)ndat / sr);

        if (sr != 1)
        {
            const double threshold = 1.6;

            return new ZoomBounds(0, threshold);


        }
        else
        {
            const double threshold = 0.5;


            return new ZoomBounds(fMin, threshold);

        }
    }

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

    public static DftComputeResult Compute(RowsCacheV2 payload, DateTime? fromUtc = null, DateTime? toUtc = null)
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

        // chave única (pmu|quantity|component|phase|unit) para não sobrescrever fases/componentes
        var specs = new Dictionary<string, Spec>(StringComparer.OrdinalIgnoreCase);
        int? ndatForZoom = null;

        foreach (var serie in orderedSeries)
        {
            var raw = serie.Points
                .Where(p => (fromUtc is null || p.Ts >= fromUtc) && (toUtc is null || p.Ts <= toUtc))
                .Select(p => new Point(p.Ts, p.Value))
                .ToList();

            if (raw.Count < 2)
                continue;

            var y = ResampleHoldLast(raw, sr);

            // Capture Ndat from the first computed series for zoom calculation
            ndatForZoom ??= y.Length;

            var spec = ForwardSingleSided(y, sr) with
            {
                Pmu = serie.IdName,
                Phase = serie.Phase,
                Component = serie.Component,
                Quantity = serie.Quantity,
                Unit = serie.Unit,
            };

            specs[BuildSeriesName(serie)] = spec;
        }

        if (specs.Count == 0)
            throw new InvalidOperationException("Nenhuma série válida para DFT.");

        // Calculate zoom bounds based on Ndat and sr
        ZoomBounds? zoom = null;
        if (ndatForZoom.HasValue)
        {
            zoom = CalculateZoomBounds(ndatForZoom.Value, sr);
        }

        return new DftComputeResult
        {
            Specs = specs,
            Zoom = zoom
        };
    }

    private static string BuildSeriesName(RowsCacheSeries s)
    {
        var pmu = (s.IdName ?? "").Trim();

        var qty = (s.Quantity ?? "").Trim().ToUpperInvariant();
        var comp = (s.Component ?? "").Trim().ToUpperInvariant();
        var ph = (s.Phase ?? "").Trim().ToUpperInvariant();

        // "PMU|VOLTAGE|MAG|A" (inclui só o que existir, mas mantém unicidade quando houver)
        var parts = new List<string>(4) { pmu };
        if (!string.IsNullOrWhiteSpace(qty)) parts.Add(qty);
        if (!string.IsNullOrWhiteSpace(comp)) parts.Add(comp);
        if (!string.IsNullOrWhiteSpace(ph)) parts.Add(ph);

        return string.Join('|', parts);
    }

    // Mantidos caso você use em outros pontos/validações futuras
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
    public Dft.ZoomBounds? Zoom { get; init; }
}