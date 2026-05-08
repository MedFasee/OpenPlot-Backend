using Complex = System.Numerics.Complex;
using MathNet.Numerics.LinearAlgebra;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Features.PostProcessing.Handlers;

public static class Prony
{
    public sealed record ModePoint(
        int Index,
        double Energy,
        double FrequencyHz,
        double DampingPercent,
        double Amplitude,
        double PhaseRad,
        double Real,
        double Imaginary);

    public sealed record TimePoint(DateTime Ts, double Value);

    public sealed record Spec(
        double Sr,
        int N,
        int Order,
        IReadOnlyList<ModePoint> Modes,
        IReadOnlyList<ModePoint> AllModes,
        IReadOnlyList<TimePoint> OriginalPoints,
        IReadOnlyList<TimePoint> EstimatedPoints)
    {
        public string? Pmu { get; init; }
        public string? Phase { get; init; }
        public string? Component { get; init; }
        public string? Quantity { get; init; }
        public string? Unit { get; init; }
    }

    private sealed record RawPoint(DateTime Ts, double Val);

    private sealed record InputSeries(RowsCacheSeries Serie, double[] Y);

    private sealed record FitResult(
        Complex[] Roots,
        Complex[] ContinuousPoles,
        double[] FrequencyHz,
        double[] DampingPercent,
        Complex[,] Residues,
        double[,] Amplitudes,
        double[,] Phases,
        double[,] Energy,
        double[,] Estimated);

    public static PronyComputeResult Compute(
        RowsCacheV2 payload,
        int order,
        DateTime? fromUtc = null,
        DateTime? toUtc = null)
    {
        if (payload is null)
            throw new ArgumentNullException(nameof(payload));

        if (payload.Series is null || payload.Series.Count == 0)
            throw new InvalidOperationException("Nenhuma série encontrada no cache.");

        if (payload.SelectRate <= 0)
            throw new InvalidOperationException("SelectRate inválido.");

        if (order <= 0)
            throw new ArgumentOutOfRangeException(nameof(order), "A ordem do Prony deve ser maior que zero.");

        var effectiveFrom = fromUtc ?? payload.From;
        var effectiveTo = toUtc ?? payload.To;

        if (effectiveFrom < payload.From) effectiveFrom = payload.From;
        if (effectiveTo > payload.To) effectiveTo = payload.To;

        if (effectiveFrom > effectiveTo)
            throw new InvalidOperationException("Janela inválida para Prony.");

        var sr = (double)payload.SelectRate;

        var orderedSeries = payload.Series
            .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var valid = new List<InputSeries>();
        DateTime[]? commonTimes = null;

        foreach (var serie in orderedSeries)
        {
            var raw = serie.Points
                .Where(p => p.Ts >= effectiveFrom && p.Ts <= effectiveTo)
                .Select(p => new RawPoint(p.Ts, p.Value))
                .ToList();

            if (raw.Count < 2)
                continue;

            var y = ResampleHoldLast(raw, sr, effectiveFrom, effectiveTo, out var times);

            if (y.Length < 2)
                continue;

            commonTimes ??= times;
            valid.Add(new InputSeries(serie, y));
        }

        if (valid.Count == 0 || commonTimes is null)
            throw new InvalidOperationException("Nenhuma série válida para Prony.");

        var n = commonTimes.Length;

        if (order >= n)
            throw new InvalidOperationException($"Ordem inválida para Prony. A ordem ({order}) deve ser menor que o número de amostras ({n}).");

        if (valid.Count * (n - order) < order)
            throw new InvalidOperationException("A janela possui poucas amostras para a ordem de Prony solicitada.");

        var fit = FitMultiSignal(valid.Select(v => v.Y).ToList(), sr, order);

        var specs = new Dictionary<string, Spec>(StringComparer.OrdinalIgnoreCase);

        for (int k = 0; k < valid.Count; k++)
        {
            var serie = valid[k].Serie;

            var allModes = BuildModesForSeries(k, order, fit, applyLegacyFilter: false);
            var modes = BuildModesForSeries(k, order, fit, applyLegacyFilter: true);

            var original = new TimePoint[n];
            var estimated = new TimePoint[n];

            for (int i = 0; i < n; i++)
            {
                original[i] = new TimePoint(commonTimes[i], valid[k].Y[i]);
                estimated[i] = new TimePoint(commonTimes[i], fit.Estimated[i, k]);
            }

            var spec = new Spec(sr, n, order, modes, allModes, original, estimated)
            {
                Pmu = serie.IdName,
                Phase = serie.Phase,
                Component = serie.Component,
                Quantity = serie.Quantity,
                Unit = serie.Unit
            };

            specs[BuildSeriesName(serie)] = spec;
        }

        var modeShapeCandidates = fit.FrequencyHz
            .Where(f => f < 10.0 && f > 1e-6)
            .Select(f => Math.Round(f, 6))
            .Distinct()
            .OrderBy(f => f)
            .ToList();

        return new PronyComputeResult
        {
            Specs = specs,
            ModeShapeCandidatesHz = modeShapeCandidates,
            FromUtc = effectiveFrom,
            ToUtc = effectiveTo
        };
    }

    private static IReadOnlyList<ModePoint> BuildModesForSeries(
        int seriesIndex,
        int order,
        FitResult fit,
        bool applyLegacyFilter)
    {
        var modes = new List<ModePoint>();

        for (int i = 0; i < order; i++)
        {
            var mode = new ModePoint(
                Index: i,
                Energy: fit.Energy[i, seriesIndex],
                FrequencyHz: fit.FrequencyHz[i],
                DampingPercent: fit.DampingPercent[i],
                Amplitude: fit.Amplitudes[i, seriesIndex],
                PhaseRad: fit.Phases[i, seriesIndex],
                Real: fit.ContinuousPoles[i].Real,
                Imaginary: fit.ContinuousPoles[i].Imaginary);

            if (!applyLegacyFilter || IsLegacyVisibleMode(mode))
                modes.Add(mode);
        }

        return applyLegacyFilter
            ? modes.OrderByDescending(m => m.Energy).ToList()
            : modes;
    }

    // Mesmo filtro usado na tabela do MedPlot:
    // frequência positiva abaixo de 10 Hz e energia acima de 1e-3.
    private static bool IsLegacyVisibleMode(ModePoint m) =>
        m.FrequencyHz < 10.0 && m.FrequencyHz > 1e-6 && m.Energy > 1e-3;

    private static FitResult FitMultiSignal(IReadOnlyList<double[]> signals, double sr, int order)
    {
        var numSignals = signals.Count;
        var n = signals[0].Length;
        var dt = 1.0 / sr;

        if (signals.Any(s => s.Length != n))
            throw new InvalidOperationException("Todas as séries devem possuir o mesmo número de amostras para o Prony multissinal.");

        var rows = numSignals * (n - order);
        var h = Matrix<double>.Build.Dense(rows, order);
        var y = Vector<double>.Build.Dense(rows);

        for (int k = 0; k < numSignals; k++)
        {
            var offset = k * (n - order);
            var signal = signals[k];

            for (int i = 0; i < n - order; i++)
            {
                for (int j = 0; j < order; j++)
                    h[i + offset, j] = signal[order - j + i - 1];

                y[i + offset] = signal[order + i];
            }
        }

        var a = SolveLeastSquares(h, y);

        var c = Matrix<Complex>.Build.Dense(order, order, Complex.Zero);
        for (int i = 0; i < order - 1; i++)
        {
            c[i, order - 1] = new Complex(a[order - i - 1], 0.0);
            c[i + 1, i] = Complex.One;
        }
        c[order - 1, order - 1] = new Complex(a[0], 0.0);

        var roots = c.Evd().EigenValues.ToArray();
        var sPoles = new Complex[order];
        var frequencyHz = new double[order];
        var dampingPercent = new double[order];

        for (int i = 0; i < order; i++)
        {
            sPoles[i] = Complex.Log(roots[i]) / dt;
            frequencyHz[i] = roots[i].Phase / (2.0 * Math.PI * dt);

            var poleAbs = sPoles[i].Magnitude;
            dampingPercent[i] = poleAbs > 0.0
                ? (-sPoles[i].Real / poleAbs) * 100.0
                : 0.0;
        }

        var z = Matrix<Complex>.Build.Dense(n, order, (i, j) => Complex.Pow(roots[j], i + 1));

        // Mantém a formulação do MedPlot: Z_inv = inv(Z^T Z) Z^T.
        // Observação: é transposta simples, não transposta conjugada.
        var zt = z.Transpose();
        var zInv = (zt * z).Inverse() * zt;

        var x = Matrix<Complex>.Build.Dense(n, numSignals, (i, k) => new Complex(signals[k][i], 0.0));
        var r = zInv * x;

        var amplitudes = new double[order, numSignals];
        var phases = new double[order, numSignals];
        var residues = new Complex[order, numSignals];
        var energy = new double[order, numSignals];
        var estimated = new double[n, numSignals];

        var t = new double[n];
        for (int i = 0; i < n; i++)
            t[i] = dt * (i + 1);

        for (int k = 0; k < numSignals; k++)
        {
            for (int mode = 0; mode < order; mode++)
            {
                residues[mode, k] = r[mode, k];
                amplitudes[mode, k] = r[mode, k].Magnitude;
                phases[mode, k] = r[mode, k].Phase;

                var sum = 0.0;
                for (int i = 0; i < n; i++)
                {
                    var contribution = amplitudes[mode, k]
                        * Math.Exp(sPoles[mode].Real * t[i])
                        * Math.Cos(2.0 * Math.PI * frequencyHz[mode] * t[i] + phases[mode, k]);

                    sum += contribution * contribution;
                }

                energy[mode, k] = sum / dt;
            }
        }

        for (int k = 0; k < numSignals; k++)
        {
            for (int i = 0; i < n; i++)
            {
                var value = 0.0;
                for (int mode = 0; mode < order; mode++)
                {
                    value += amplitudes[mode, k]
                        * Math.Exp(sPoles[mode].Real * t[i])
                        * Math.Cos(2.0 * Math.PI * frequencyHz[mode] * t[i] + phases[mode, k]);
                }

                estimated[i, k] = value;
            }
        }

        return new FitResult(
            roots,
            sPoles,
            frequencyHz,
            dampingPercent,
            residues,
            amplitudes,
            phases,
            energy,
            estimated);
    }

    private static Vector<double> SolveLeastSquares(Matrix<double> h, Vector<double> y)
    {
        try
        {
            return h.QR().Solve(y);
        }
        catch
        {
            var ht = h.Transpose();
            return (ht * h).Solve(ht * y);
        }
    }

    // Resample hold-last em uma grade temporal comum para todas as séries.
    private static double[] ResampleHoldLast(
        IReadOnlyList<RawPoint> raw,
        double sr,
        DateTime fromUtc,
        DateTime toUtc,
        out DateTime[] times)
    {
        if (raw.Count == 0)
        {
            times = Array.Empty<DateTime>();
            return Array.Empty<double>();
        }

        var pts = raw.OrderBy(p => p.Ts).ToList();

        var dtTicks = (long)Math.Round(TimeSpan.TicksPerSecond / sr);
        if (dtTicks <= 0) dtTicks = 1;

        var spanTicks = (toUtc - fromUtc).Ticks;
        var n = (int)(spanTicks / dtTicks) + 1;
        if (n < 2) n = 2;

        var y = new double[n];
        times = new DateTime[n];

        int j = 0;
        double last = pts[0].Val;

        for (int i = 0; i < n; i++)
        {
            var ti = fromUtc.AddTicks(i * dtTicks);
            times[i] = ti;

            while (j < pts.Count && pts[j].Ts <= ti)
            {
                last = pts[j].Val;
                j++;
            }

            y[i] = last;
        }

        return y;
    }

    private static string BuildSeriesName(RowsCacheSeries s)
    {
        var pmu = (s.IdName ?? "").Trim();
        var qty = (s.Quantity ?? "").Trim().ToUpperInvariant();
        var comp = (s.Component ?? "").Trim().ToUpperInvariant();
        var ph = (s.Phase ?? "").Trim().ToUpperInvariant();

        var parts = new List<string>(4) { pmu };
        if (!string.IsNullOrWhiteSpace(qty)) parts.Add(qty);
        if (!string.IsNullOrWhiteSpace(comp)) parts.Add(comp);
        if (!string.IsNullOrWhiteSpace(ph)) parts.Add(ph);

        return string.Join('|', parts);
    }
}

public sealed class PronyComputeResult
{
    public Dictionary<string, Prony.Spec> Specs { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public IReadOnlyList<double> ModeShapeCandidatesHz { get; init; } = Array.Empty<double>();
    public DateTime FromUtc { get; init; }
    public DateTime ToUtc { get; init; }
}
