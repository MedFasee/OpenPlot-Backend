using Complex = System.Numerics.Complex;
using MathNet.Numerics.LinearAlgebra;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Features.PostProcessing.Handlers;

public static class Prony
{
    public sealed record ModeShapeVectorPoint(
        string Series,
        string? Pmu,
        double Phase,
        string? Component,
        string? Quantity,
        string? Unit,
        double Amplitude,
        double PhaseRad);

    public sealed record ModeShapeCandidate(
        int Index,
        double FrequencyHz,
        IReadOnlyList<ModeShapeVectorPoint> Vector);

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

    private sealed record PreparedSeries(RowsCacheSeries Serie, List<RawPoint> Raw);

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

        var dtTicks = Math.Max(1L, (long)Math.Round(TimeSpan.TicksPerSecond / sr));
        var exactToleranceTicks = Math.Max(1L, dtTicks / 4L);
        var prepared = orderedSeries
            .Select(serie => new PreparedSeries(
                serie,
                serie.Points
                    .Where(p => p.Ts >= effectiveFrom && p.Ts <= effectiveTo)
                    .Select(p => new RawPoint(p.Ts, p.Value))
                    .OrderBy(p => p.Ts)
                    .ToList()))
            .Where(p => p.Raw.Count >= 2)
            .ToList();

        var valid = new List<InputSeries>();
        DateTime[] commonTimes;

        if (TryBuildExactAlignedInputs(prepared, exactToleranceTicks, out commonTimes, out valid))
        {
            // Usa exatamente as amostras reais alinhadas do cache, como no MedPlot.
        }
        else
        {
            commonTimes = BuildUniformTimes(sr, effectiveFrom, effectiveTo);

            foreach (var serie in prepared)
            {
                if (!CoversWindow(serie.Raw, commonTimes, exactToleranceTicks))
                    continue;

                double[] y;
                if (!TryProjectExactSamples(serie.Raw, commonTimes, exactToleranceTicks, out y))
                    y = ResampleHoldLast(serie.Raw, commonTimes);

                if (y.Length < 2)
                    continue;

                valid.Add(new InputSeries(serie.Serie, y));
            }
        }

        if (valid.Count == 0)
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

            var allModes = BuildAllModesForSeries(k, order, fit);
            var modes = BuildLegacyVisibleModesForSeries(k, order, fit);

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

        var modeShapeCandidates = BuildLegacyModeShapeCandidates(valid, fit, order);

        return new PronyComputeResult
        {
            Specs = specs,
            ModeShapeCandidatesHz = modeShapeCandidates,
            FromUtc = effectiveFrom,
            ToUtc = effectiveTo
        };
    }

    private static IReadOnlyList<ModePoint> BuildAllModesForSeries(
        int seriesIndex,
        int order,
        FitResult fit)
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

            modes.Add(mode);
        }

        return modes;
    }

    private static IReadOnlyList<ModePoint> BuildLegacyVisibleModesForSeries(
        int seriesIndex,
        int order,
        FitResult fit) =>
        BuildAllModesForSeries(seriesIndex, order, fit)
            .Where(IsLegacyVisibleMode)
            .OrderByDescending(m => m.Energy)
            .ToList();

    private static IReadOnlyList<ModeShapeCandidate> BuildLegacyModeShapeCandidates(
        IReadOnlyList<InputSeries> valid,
        FitResult fit,
        int order)
    {
        if (valid.Count < 2)
            return Array.Empty<ModeShapeCandidate>();

        return Enumerable.Range(0, order)
            .Where(modeIndex => IsLegacyModeShapeCandidate(fit.FrequencyHz[modeIndex]))
            .Select(modeIndex => new ModeShapeCandidate(
                Index: modeIndex,
                FrequencyHz: fit.FrequencyHz[modeIndex],
                Vector: BuildModeShapeVector(valid, fit, modeIndex)))
            .OrderBy(candidate => candidate.FrequencyHz)
            .ToList();
    }

    private static IReadOnlyList<ModeShapeVectorPoint> BuildModeShapeVector(
        IReadOnlyList<InputSeries> valid,
        FitResult fit,
        int modeIndex)
    {
        var vector = new List<ModeShapeVectorPoint>(valid.Count);

        for (int seriesIndex = 0; seriesIndex < valid.Count; seriesIndex++)
        {
            var serie = valid[seriesIndex].Serie;
            vector.Add(new ModeShapeVectorPoint(
                Series: BuildModeShapeSeriesName(serie),
                Pmu: serie.IdName,
                Phase: RadiansToDegrees(fit.Phases[modeIndex, seriesIndex]),
                Component: serie.Component,
                Quantity: serie.Quantity,
                Unit: serie.Unit,
                Amplitude: fit.Amplitudes[modeIndex, seriesIndex],
                PhaseRad: fit.Phases[modeIndex, seriesIndex]));
        }

        return vector;
    }

    private static string BuildModeShapeSeriesName(RowsCacheSeries s) =>
        (s.IdName ?? string.Empty).Trim();

    private static double RadiansToDegrees(double radians) =>
        radians * (180.0 / Math.PI);

    // Mesmo filtro usado na tabela do MedPlot:
    // frequência positiva abaixo de 10 Hz e energia acima de 1e-3.
    private static bool IsLegacyVisibleMode(ModePoint m) =>
        m.FrequencyHz < 10.0 && m.FrequencyHz > 1e-6 && m.Energy > 1e-3;

    // No MedPlot, as possibilidades de mode shape são montadas apenas pelo critério
    // de frequência positiva abaixo de 10 Hz, preservando repetições e ordenando.
    private static bool IsLegacyModeShapeCandidate(double frequencyHz) =>
        frequencyHz < 10.0 && frequencyHz > 1e-6;

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

        var zInv = SolveLegacyPseudoInverse(z);

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

    private static Matrix<Complex> SolveLegacyPseudoInverse(Matrix<Complex> z)
    {
        var order = z.ColumnCount;
        var zt = z.Transpose();
        var p = zt * z;

        var q = Matrix<double>.Build.Dense(order * 2, order * 2);
        for (int i = 0; i < order; i++)
        {
            for (int j = 0; j < order; j++)
            {
                q[i, j] = p[i, j].Real;
                q[i + order, j] = -p[i, j].Imaginary;
                q[i, j + order] = p[i, j].Imaginary;
                q[i + order, j + order] = p[i, j].Real;
            }
        }

        var qInv = q.Inverse();
        var qComplex = Matrix<Complex>.Build.Dense(order, order, (i, j) =>
            new Complex(qInv[i, j], qInv[i, j + order]));

        return qComplex * zt;
    }

    private static DateTime[] BuildUniformTimes(
        double sr,
        DateTime fromUtc,
        DateTime toUtc)
    {
        var dtTicks = (long)Math.Round(TimeSpan.TicksPerSecond / sr);
        if (dtTicks <= 0) dtTicks = 1;

        var spanTicks = (toUtc - fromUtc).Ticks;
        var n = (int)(spanTicks / dtTicks) + 1;
        if (n < 2) n = 2;

        var times = new DateTime[n];
        for (int i = 0; i < n; i++)
            times[i] = fromUtc.AddTicks(i * dtTicks);

        return times;
    }

    private static bool TryBuildExactAlignedInputs(
        IReadOnlyList<PreparedSeries> prepared,
        long toleranceTicks,
        out DateTime[] commonTimes,
        out List<InputSeries> valid)
    {
        commonTimes = Array.Empty<DateTime>();
        valid = new List<InputSeries>();

        if (prepared.Count == 0)
            return false;

        DateTime[]? bestTimes = null;
        List<InputSeries>? bestValid = null;

        foreach (var candidate in prepared.OrderByDescending(p => p.Raw.Count))
        {
            var times = candidate.Raw.Select(p => p.Ts).ToArray();
            var aligned = new List<InputSeries>();

            foreach (var serie in prepared)
            {
                if (TryProjectExactSamples(serie.Raw, times, toleranceTicks, out var y))
                    aligned.Add(new InputSeries(serie.Serie, y));
            }

            if (aligned.Count == 0)
                continue;

            if (bestValid is null
                || aligned.Count > bestValid.Count
                || (aligned.Count == bestValid.Count && times.Length > bestTimes!.Length))
            {
                bestTimes = times;
                bestValid = aligned;
            }
        }

        if (bestTimes is null || bestValid is null)
            return false;

        commonTimes = bestTimes;
        valid = bestValid;
        return true;
    }

    private static bool TryProjectExactSamples(
        IReadOnlyList<RawPoint> raw,
        IReadOnlyList<DateTime> times,
        long toleranceTicks,
        out double[] values)
    {
        values = Array.Empty<double>();

        if (raw.Count != times.Count)
            return false;

        var pts = raw.OrderBy(p => p.Ts).ToList();
        var projected = new double[times.Count];

        for (int i = 0; i < times.Count; i++)
        {
            var delta = Math.Abs((pts[i].Ts - times[i]).Ticks);
            if (delta > toleranceTicks)
                return false;

            projected[i] = pts[i].Val;
        }

        values = projected;
        return true;
    }

    private static bool CoversWindow(
        IReadOnlyList<RawPoint> raw,
        IReadOnlyList<DateTime> times,
        long toleranceTicks)
    {
        if (raw.Count == 0 || times.Count == 0)
            return false;

        var firstDelta = raw[0].Ts.Ticks - times[0].Ticks;
        if (firstDelta > toleranceTicks)
            return false;

        var lastDelta = times[^1].Ticks - raw[^1].Ts.Ticks;
        return lastDelta <= toleranceTicks;
    }

    // Resample hold-last em uma grade temporal comum para todas as séries.
    private static double[] ResampleHoldLast(
        IReadOnlyList<RawPoint> raw,
        IReadOnlyList<DateTime> times)
    {
        if (raw.Count == 0 || times.Count == 0)
            return Array.Empty<double>();

        var pts = raw.OrderBy(p => p.Ts).ToList();
        var n = times.Count;
        var y = new double[n];

        int j = 0;
        double last = pts[0].Val;

        for (int i = 0; i < n; i++)
        {
            var ti = times[i];

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
    public IReadOnlyList<Prony.ModeShapeCandidate> ModeShapeCandidatesHz { get; init; } = Array.Empty<Prony.ModeShapeCandidate>();
    public DateTime FromUtc { get; init; }
    public DateTime ToUtc { get; init; }
}
