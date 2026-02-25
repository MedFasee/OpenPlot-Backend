using MathNet.Numerics.IntegralTransforms;
using OpenPlot.Core.TimeSeries;
using System.Numerics;

namespace OpenPlot.Features.Runs.Calculations;

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

    // Resample “hold-last” num grid uniforme (recomendado pro DFT)
    // Retorna y[] com N pontos igualmente espaçados em dt = 1/sr.
    public static double[] ResampleHoldLast(IReadOnlyList<Point> raw, double sr, int? nMax = null)
    {
        if (raw.Count == 0) return Array.Empty<double>();

        // garante ordenação
        var pts = raw.OrderBy(p => p.Ts).ToList();

        // se quiser limitar N (performance)
        if (nMax is > 0 && pts.Count > nMax.Value)
            pts = pts.Skip(pts.Count - nMax.Value).ToList();

        var dtTicks = (long)Math.Round(TimeSpan.TicksPerSecond / sr);
        if (dtTicks <= 0) dtTicks = 1;

        var t0 = pts[0].Ts;
        var tN = pts[^1].Ts;
        var spanTicks = (tN - t0).Ticks;

        // número de amostras no grid (inclui o primeiro)
        var n = (int)(spanTicks / dtTicks) + 1;
        if (n < 2) n = 2;

        var y = new double[n];

        int j = 0;
        double last = pts[0].Val;

        for (int i = 0; i < n; i++)
        {
            var ti = t0.AddTicks(i * dtTicks);

            // avança enquanto ponto <= ti
            while (j < pts.Count && pts[j].Ts <= ti)
            {
                last = pts[j].Val;
                j++;
            }

            y[i] = last; // hold-last
        }

        return y;
    }
}