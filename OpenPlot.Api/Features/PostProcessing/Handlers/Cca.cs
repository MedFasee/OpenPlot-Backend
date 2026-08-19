using System.Numerics;
using MathNet.Filtering.FIR;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Factorization;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Features.PostProcessing.Handlers;

public static class Cca
{
    private const int MovingAverageOrder = 20;
    private const double OutlierThreshold = 5.0;
    private const double FilteredSamplingRateHz = 5.0;
    private const double MaxAcceptedDampingPercent = 30.0;

    public sealed record ModeShapePoint(
        string Series,
        string? Pmu,
        double Amplitude,
        double Phase,
        double PhaseRad,
        string? Component,
        string? Quantity,
        string? Unit);

    public sealed record DominantMode(
        int Index,
        double FrequencyHz,
        double DampingPercent,
        double Score,
        IReadOnlyList<ModeShapePoint> Vector);

    public sealed record ModePoint(
        int Index,
        double FrequencyHz,
        double DampingPercent,
        double PseudoEnergy,
        double Idm,
        double Real,
        double Imaginary);

    public sealed record WindowPoint(
        int Index,
        DateTime FromUtc,
        DateTime ToUtc,
        DominantMode Energy,
        DominantMode Idm,
        IReadOnlyList<ModePoint> AllModes);

    private sealed record RawPoint(DateTime Ts, double Value);
    private sealed record InputSeries(RowsCacheSeries Series, double[] Samples);

    public static CcaComputeResult Compute(
        RowsCacheV2 payload,
        int modelOrder,
        int blockRows,
        int windowLengthMinutes,
        int windowStepSeconds,
        double frequencyMinHz,
        double frequencyMaxHz,
        DateTime? fromUtc = null,
        DateTime? toUtc = null)
    {
        ArgumentNullException.ThrowIfNull(payload);

        if (payload.Series is null || payload.Series.Count == 0)
            throw new InvalidOperationException("Nenhuma série encontrada no cache.");

        if (payload.SelectRate <= 0)
            throw new InvalidOperationException("SelectRate inválido.");

        if (modelOrder <= 0)
            throw new ArgumentOutOfRangeException(nameof(modelOrder), "A ordem do modelo do CCA deve ser maior que zero.");

        if (blockRows <= 0)
            throw new ArgumentOutOfRangeException(nameof(blockRows), "O número de linhas por bloco do CCA deve ser maior que zero.");

        if (windowLengthMinutes <= 0)
            throw new ArgumentOutOfRangeException(nameof(windowLengthMinutes), "O tamanho da janela do CCA deve ser maior que zero.");

        if (windowStepSeconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(windowStepSeconds), "O passo da janela do CCA deve ser maior que zero.");

        if (frequencyMinHz <= 0)
            throw new ArgumentOutOfRangeException(nameof(frequencyMinHz), "A frequência mínima do CCA deve ser maior que zero.");

        if (frequencyMaxHz <= frequencyMinHz)
            throw new ArgumentOutOfRangeException(nameof(frequencyMaxHz), "A frequência máxima do CCA deve ser maior que a frequência mínima.");

        var effectiveFrom = fromUtc ?? payload.From;
        var effectiveTo = toUtc ?? payload.To;

        if (effectiveFrom < payload.From) effectiveFrom = payload.From;
        if (effectiveTo > payload.To) effectiveTo = payload.To;

        if (effectiveFrom >= effectiveTo)
            throw new InvalidOperationException("Janela inválida para CCA.");

        var sr = payload.SelectRate;
        var commonTimes = BuildUniformTimes(sr, effectiveFrom, effectiveTo);
        var inputs = PrepareInputs(payload.Series, commonTimes);

        if (inputs.Count == 0)
            throw new InvalidOperationException("Nenhuma série válida para CCA.");

        var availablePointCount = inputs.Min(input => input.Samples.Length);
        var windowPointCount = checked(windowLengthMinutes * 60 * sr);

        if (windowPointCount <= 2 * blockRows)
            throw new InvalidOperationException("O número de linhas por bloco deve ser inferior à metade do número de pontos da janela deslizante.");

        if (windowPointCount > availablePointCount)
            throw new InvalidOperationException("O tamanho da janela deslizante deve ser inferior ao tamanho do período total do sinal.");

        var windowStepSamples = Math.Max(1, checked(windowStepSeconds * sr));
        var executionCount = ((availablePointCount - windowPointCount) / windowStepSamples) + 1;
        if (executionCount <= 0)
            throw new InvalidOperationException("A consulta não possui pontos suficientes para executar o CCA.");

        if (modelOrder > blockRows * inputs.Count)
            throw new InvalidOperationException("A ordem do modelo do CCA deve ser menor ou igual ao produto entre o número de linhas por bloco e a quantidade de séries válidas.");

        var windows = new List<WindowPoint>(executionCount);

        for (var executionIndex = 0; executionIndex < executionCount; executionIndex++)
        {
            var startIndex = executionIndex * windowStepSamples;
            var endIndex = startIndex + windowPointCount - 1;
            var windowStart = commonTimes[startIndex];
            var windowEnd = commonTimes[endIndex];

            var windowSignals = inputs
                .Select(input => new InputSeries(
                    input.Series,
                    input.Samples.Skip(startIndex).Take(windowPointCount).ToArray()))
                .ToList();

            var processedSignals = PreprocessSignals(windowSignals, sr);
            if (processedSignals.Count == 0)
                continue;

            var modalResult = ComputeWindowModes(
                processedSignals,
                modelOrder,
                blockRows,
                frequencyMinHz,
                frequencyMaxHz);

            windows.Add(new WindowPoint(
                Index: executionIndex,
                FromUtc: windowStart,
                ToUtc: windowEnd,
                Energy: BuildDominantMode(processedSignals, modalResult, modalResult.MaxEnergyIndex, useIdm: false),
                Idm: BuildDominantMode(processedSignals, modalResult, modalResult.MaxIdmIndex, useIdm: true),
                AllModes: BuildAllModes(modalResult)));
        }

        if (windows.Count == 0)
            throw new InvalidOperationException("Nenhuma janela válida foi produzida para o CCA.");

        return new CcaComputeResult
        {
            Windows = windows,
            FromUtc = effectiveFrom,
            ToUtc = effectiveTo,
            Parameters = new CcaParameters(
                modelOrder,
                blockRows,
                windowLengthMinutes,
                windowStepSeconds,
                frequencyMinHz,
                frequencyMaxHz)
        };
    }

    private static List<InputSeries> PrepareInputs(IReadOnlyList<RowsCacheSeries> series, IReadOnlyList<DateTime> commonTimes)
    {
        return series
            .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
            .Select(seriesItem =>
            {
                var raw = seriesItem.Points
                    .Select(point => new RawPoint(point.Ts, point.Value))
                    .OrderBy(point => point.Ts)
                    .ToList();

                var resampled = ResampleHoldLast(raw, commonTimes);
                return resampled.Length == commonTimes.Count
                    ? new InputSeries(seriesItem, resampled)
                    : null;
            })
            .Where(item => item is not null)
            .Cast<InputSeries>()
            .ToList();
    }

    private static List<InputSeries> PreprocessSignals(IReadOnlyList<InputSeries> signals, int samplingRate)
    {
        // GraficoCVA:
        // int d = Convert.ToInt16(sr / fDown);
        var downsampleFactor = Convert.ToInt16(samplingRate / FilteredSamplingRateHz);

        if (downsampleFactor <= 0)
            throw new InvalidOperationException("Fator de downsampling inválido para o CCA.");

        var processed = new List<InputSeries>(signals.Count);

        foreach (var signal in signals)
        {
            var samples = signal.Samples.ToArray();

            // 1) Média móvel — mesma implementação do GraficoCVA.
            var movingAverage = MovingAverage(samples);

            // 2) Desvio padrão amostral.
            var standardDeviation = StandardDeviation(samples);

            // 3) Identificação de outliers — mesma expressão do GraficoCVA.
            var withoutOutliers = IdentifyOutliers(
                samples,
                movingAverage,
                standardDeviation,
                OutlierThreshold);

            // 4) Interpolação dos outliers — mesma lógica do GraficoCVA.
            InterpolateNaNs(withoutOutliers);

            // 5) Remoção da média.
            RemoveAverage(withoutOutliers);

            // 6) Filtro FIR passa-banda EXATO usado no GraficoCVA:
            //    FirCoefficients.BandPass(sr, 0.15, 2, 10)
            //    OnlineFirFilter.ProcessSamples(...)
            var coefficients = FirCoefficients.BandPass(
                samplingRate,
                0.15,
                2.0,
                10);

            var firFilter = new OnlineFirFilter(coefficients);
            var filtered = firFilter.ProcessSamples(withoutOutliers);

            // 7) Downsampling após o FIR, como no legado.
            var downsampled = Downsample(filtered, downsampleFactor);

            if (downsampled.Length >= 3)
                processed.Add(new InputSeries(signal.Series, downsampled));
        }

        return processed;
    }

    private static WindowModalResult ComputeWindowModes(
        IReadOnlyList<InputSeries> signals,
        int modelOrder,
        int blockRows,
        double frequencyMinHz,
        double frequencyMaxHz)
    {
        var p = signals.Count;
        var signalLength = signals[0].Samples.Length;
        var effectiveSamplingRate = Math.Min(FilteredSamplingRateHz, signals[0].Samples.Length > 0 ? FilteredSamplingRateHz : 1.0);

        if (signals.Any(signal => signal.Samples.Length != signalLength))
            throw new InvalidOperationException("Todas as séries do CCA devem possuir o mesmo número de amostras processadas.");

        var n = signalLength - (2 * blockRows);
        if (n <= 0)
            throw new InvalidOperationException("A janela processada do CCA não comporta o número de linhas por bloco informado.");

        if (modelOrder > blockRows * p)
            throw new InvalidOperationException("A ordem do modelo do CCA excede a dimensão disponível na janela processada.");

        var y = Matrix<double>.Build.Dense(2 * blockRows * p, n);

        for (var blockIndex = 0; blockIndex < 2 * blockRows; blockIndex++)
        {
            for (var column = 0; column < n; column++)
            {
                for (var signalIndex = 0; signalIndex < p; signalIndex++)
                    y[(blockIndex * p) + signalIndex, column] = signals[signalIndex].Samples[blockIndex + column];
            }
        }

        var blockDimension = blockRows * p;

        var yp = y.SubMatrix(0, blockDimension, 0, n);
        var yf = y.SubMatrix(blockDimension, blockDimension, 0, n);

        // GraficoCVA:
        // H = [Yp; Yf]
        // [Q,L] = qr(H',0)
        //
        // Como Y já é exatamente [Yp;Yf], H == Y.
        var hTranspose = y.Transpose();

        // Economy-size QR, equivalente ao qr(H',0).
        var qr = hTranspose.QR(QRMethod.Thin);
        var l = qr.R;

        if (l.RowCount < 2 * blockDimension || l.ColumnCount < 2 * blockDimension)
        {
            throw new InvalidOperationException(
                $"A janela processada é insuficiente para a decomposição QR do CCA. " +
                $"R={l.RowCount}x{l.ColumnCount}; necessário pelo menos " +
                $"{2 * blockDimension}x{2 * blockDimension}.");
        }

        // Ltr = L' / sqrt(N)
        var lSquare = l.SubMatrix(
            0,
            2 * blockDimension,
            0,
            2 * blockDimension);

        var ltr = lSquare.Transpose() / Math.Sqrt(n);

        var l11 = ltr.SubMatrix(
            0,
            blockDimension,
            0,
            blockDimension);

        var l21 = ltr.SubMatrix(
            blockDimension,
            blockDimension,
            0,
            blockDimension);

        var l22 = ltr.SubMatrix(
            blockDimension,
            blockDimension,
            blockDimension,
            blockDimension);

        // Mesmas matrizes de covariância do GraficoCVA.
        var rff = (l21 * l21.Transpose()) + (l22 * l22.Transpose());
        var rfp = l21 * l11.Transpose();
        var rpp = l11 * l11.Transpose();

        var t = MatrixSquareRoot(rff);
        var tInv = MatrixSquareRootInverse(rff);
        var mInv = MatrixSquareRootInverse(rpp);
        var oc = tInv * rfp * mInv.Transpose();

        var ocSvd = oc.Svd(computeVectors: true);
        var u = ocSvd.U;
        var singularValues = ocSvd.S;

        var reducedSingular = Matrix<double>.Build.DenseDiagonal(modelOrder, modelOrder, index => Math.Sqrt(Math.Max(singularValues[index], 0.0)));
        var reducedU = u.SubMatrix(0, u.RowCount, 0, modelOrder);
        var observability = t * reducedU * reducedSingular;

        var numerator = observability.SubMatrix(0, (blockRows * p) - p, 0, modelOrder);
        var denominator = observability.SubMatrix(p, (blockRows * p) - p, 0, modelOrder);
        var systemMatrix = SolveLeastSquares(numerator, denominator);
        var systemMatrixComplex = Matrix<Complex>.Build.Dense(
            systemMatrix.RowCount,
            systemMatrix.ColumnCount,
            (row, column) => new Complex(systemMatrix[row, column], 0.0));

        var eigen = systemMatrixComplex.Evd();
        var eigenValues = eigen.EigenValues.ToArray();
        var eigenVectors = eigen.EigenVectors;
        var continuousPoles = new Complex[modelOrder];
        var frequencies = new double[modelOrder];
        var damping = new double[modelOrder];

        for (var i = 0; i < modelOrder; i++)
        {
            continuousPoles[i] = Complex.Log(eigenValues[i]) * effectiveSamplingRate;
            frequencies[i] = eigenValues[i].Phase * effectiveSamplingRate / (2.0 * Math.PI);

            var poleMagnitude = continuousPoles[i].Magnitude;
            damping[i] = poleMagnitude > 0.0
                ? (-continuousPoles[i].Real / poleMagnitude) * 100.0
                : 0.0;
        }

        var z = Matrix<Complex>.Build.Dense(signalLength, modelOrder, (row, column) => Complex.Pow(eigenValues[column], row));
        var zPseudoInverse = SolvePseudoInverse(z);
        var x = Matrix<Complex>.Build.Dense(signalLength, p, (row, column) => new Complex(signals[column].Samples[row], 0.0));
        var residues = zPseudoInverse * x;

        var pseudoEnergy = new double[modelOrder];
        var idm = new double[modelOrder];

        for (var modeIndex = 0; modeIndex < modelOrder; modeIndex++)
        {
            var sum = 0.0;
            for (var sampleIndex = 0; sampleIndex < signalLength; sampleIndex++)
                sum += Complex.Abs(Complex.Pow(eigenValues[modeIndex], sampleIndex)) * Complex.Abs(Complex.Pow(eigenValues[modeIndex], sampleIndex));

            var idmAccumulator = 0.0;
            for (var signalIndex = 0; signalIndex < p; signalIndex++)
            {
                var residue = residues[modeIndex, signalIndex];
                pseudoEnergy[modeIndex] += (Complex.Conjugate(residue) * residue).Real * sum;

                var denominatorValue = (continuousPoles[modeIndex] * Complex.Conjugate(continuousPoles[modeIndex])).Real;
                if (denominatorValue > 0.0)
                {
                    var dominance = Math.Abs((-(residue * Complex.Conjugate(continuousPoles[modeIndex]))).Real / denominatorValue);
                    idmAccumulator += dominance;
                }
            }

            idm[modeIndex] = p > 0 ? idmAccumulator / p : 0.0;

            if (frequencies[modeIndex] < frequencyMinHz
                || frequencies[modeIndex] > frequencyMaxHz
                || damping[modeIndex] > MaxAcceptedDampingPercent)
            {
                pseudoEnergy[modeIndex] = 0.0;
                idm[modeIndex] = 0.0;
            }
        }

        var maxEnergyIndex = IndexOfMax(pseudoEnergy);
        var maxIdmIndex = IndexOfMax(idm);
        var outputMatrix = Matrix<Complex>.Build.Dense(p, modelOrder, (row, column) => new Complex(observability[row, column], 0.0));

        return new WindowModalResult(
            EigenValues: eigenValues,
            ContinuousPoles: continuousPoles,
            Frequencies: frequencies,
            Damping: damping,
            Residues: residues,
            PseudoEnergy: pseudoEnergy,
            Idm: idm,
            EigenVectors: eigenVectors,
            OutputMatrix: outputMatrix,
            MaxEnergyIndex: maxEnergyIndex,
            MaxIdmIndex: maxIdmIndex);
    }

    private static IReadOnlyList<ModePoint> BuildAllModes(WindowModalResult result)
    {
        var modes = new List<ModePoint>(result.Frequencies.Length);

        for (var i = 0; i < result.Frequencies.Length; i++)
        {
            modes.Add(new ModePoint(
                Index: i,
                FrequencyHz: result.Frequencies[i],
                DampingPercent: result.Damping[i],
                PseudoEnergy: result.PseudoEnergy[i],
                Idm: result.Idm[i],
                Real: result.ContinuousPoles[i].Real,
                Imaginary: result.ContinuousPoles[i].Imaginary));
        }

        return modes;
    }

    private static DominantMode BuildDominantMode(
        IReadOnlyList<InputSeries> signals,
        WindowModalResult result,
        int modeIndex,
        bool useIdm)
    {
        var score = useIdm ? result.Idm[modeIndex] : result.PseudoEnergy[modeIndex];
        var modeShape = result.OutputMatrix * result.EigenVectors.Column(modeIndex).ToColumnMatrix();
        var vector = new List<ModeShapePoint>(signals.Count);

        for (var seriesIndex = 0; seriesIndex < signals.Count; seriesIndex++)
        {
            var series = signals[seriesIndex].Series;
            var value = modeShape[seriesIndex, 0];

            vector.Add(new ModeShapePoint(
                Series: BuildSeriesName(series),
                Pmu: series.IdName,
                Amplitude: value.Magnitude,
                Phase: value.Phase * (180.0 / Math.PI),
                PhaseRad: value.Phase,
                Component: series.Component,
                Quantity: series.Quantity,
                Unit: series.Unit));
        }

        return new DominantMode(
            Index: modeIndex,
            FrequencyHz: result.Frequencies[modeIndex],
            DampingPercent: result.Damping[modeIndex],
            Score: score,
            Vector: vector);
    }

    private static Matrix<double> MatrixSquareRoot(Matrix<double> matrix)
    {
        var svd = matrix.Svd(computeVectors: true);
        var sqrtS = Matrix<double>.Build.DenseDiagonal(svd.S.Count, svd.S.Count, index => Math.Sqrt(Math.Max(svd.S[index], 0.0)));
        return svd.U * sqrtS * svd.VT;
    }

    private static Matrix<double> MatrixSquareRootInverse(Matrix<double> matrix)
    {
        var svd = matrix.Svd(computeVectors: true);

        var inverseSqrt = Matrix<double>.Build.DenseDiagonal(
            svd.S.Count,
            svd.S.Count,
            index =>
            {
                var singularValue = svd.S[index];

                // GraficoCVA:
                // sqrt(S) -> inverse().
                //
                // Não existe cutoff fixo em 1e-12. Valores positivos,
                // mesmo muito pequenos, participam da solução.
                if (singularValue == 0.0)
                    throw new InvalidOperationException(
                        $"Matriz singular: valor singular zero no índice {index}.");

                return 1.0 / Math.Sqrt(singularValue);
            });

        return svd.VT.Transpose() * inverseSqrt * svd.U.Transpose();
    }

    private static Matrix<double> SolveLeastSquares(Matrix<double> a, Matrix<double> b)
    {
        // Correspondência do LinearLeastSquares.COFSolve do GraficoCVA.
        // Sem fallback para (A'A)^-1 A'B, pois isso muda numericamente o método.
        return a.QR(QRMethod.Thin).Solve(b);
    }

    private static Matrix<Complex> SolvePseudoInverse(Matrix<Complex> matrix)
    {
        // GraficoCVA:
        // Z_inv = inv(Zᴴ Z) Zᴴ
        //
        // Sem regularização artificial.
        var conjugateTranspose = matrix.ConjugateTranspose();
        return (conjugateTranspose * matrix).Inverse() * conjugateTranspose;
    }

    private static DateTime[] BuildUniformTimes(int samplingRate, DateTime fromUtc, DateTime toUtc)
    {
        var ticksPerSample = Math.Max(1L, (long)Math.Round(TimeSpan.TicksPerSecond / (double)samplingRate));
        var spanTicks = (toUtc - fromUtc).Ticks;
        var count = (int)(spanTicks / ticksPerSample) + 1;
        if (count < 2)
            count = 2;

        var times = new DateTime[count];
        for (var i = 0; i < count; i++)
            times[i] = fromUtc.AddTicks(i * ticksPerSample);

        return times;
    }

    private static double[] ResampleHoldLast(IReadOnlyList<RawPoint> raw, IReadOnlyList<DateTime> times)
    {
        if (raw.Count == 0 || times.Count == 0)
            return Array.Empty<double>();

        var values = new double[times.Count];
        var index = 0;
        var lastValue = raw[0].Value;

        for (var i = 0; i < times.Count; i++)
        {
            while (index < raw.Count && raw[index].Ts <= times[i])
            {
                lastValue = raw[index].Value;
                index++;
            }

            values[i] = lastValue;
        }

        return values;
    }

    private static double[] MovingAverage(double[] values)
    {
        double movingSum = 0.0;
        var movingAverage = new double[values.Length];

        try
        {
            for (var j = 0; j < values.Length; j++)
            {
                if (j == 0)
                {
                    movingAverage[j] = values[j];
                }
                else if (j > 0 && j < MovingAverageOrder / 2)
                {
                    movingSum = 0.0;

                    for (var k = 1; k < j; k++)
                        movingSum += values[j - k];

                    for (var k = 0; k < MovingAverageOrder / 2; k++)
                        movingSum += values[j + k];

                    movingAverage[j] =
                        movingSum / (j + (MovingAverageOrder / 2) - 1);
                }
                else if (j >= MovingAverageOrder / 2
                         && j < values.Length - MovingAverageOrder / 2)
                {
                    movingSum = 0.0;

                    for (var k = 1; k <= MovingAverageOrder / 2; k++)
                    {
                        movingSum += values[j - k];
                        movingSum += values[j + k];
                    }

                    movingSum += values[j];

                    movingAverage[j] =
                        movingSum / (MovingAverageOrder + 1);
                }
                else if (j >= values.Length - MovingAverageOrder / 2)
                {
                    movingSum = 0.0;

                    for (var k = 1; k < values.Length - j; k++)
                        movingSum += values[j + k];

                    for (var k = 0; k < MovingAverageOrder / 2; k++)
                        movingSum += values[j - k];

                    movingAverage[j] =
                        movingSum /
                        (values.Length - j + (MovingAverageOrder / 2) - 1);
                }
            }

            return movingAverage;
        }
        catch
        {
            return movingAverage;
        }
    }

    private static double StandardDeviation(double[] values)
    {
        if (values.Length <= 1)
            return 0.0;

        var average = values.Average();
        var variance = values.Sum(value => Math.Pow(value - average, 2)) / (values.Length - 1);
        return Math.Sqrt(Math.Max(variance, 0.0));
    }

    private static double[] IdentifyOutliers(
        double[] values,
        double[] movingAverage,
        double standardDeviation,
        double threshold)
    {
        var output = values.ToArray();

        try
        {
            for (var j = 0; j < output.Length; j++)
            {
                if ((Math.Abs(output[j]) >
                     Math.Abs(movingAverage[j]) + threshold * standardDeviation)
                    ||
                    (Math.Abs(output[j]) <
                     Math.Abs(movingAverage[j]) - threshold * standardDeviation))
                {
                    output[j] = double.NaN;
                }
            }

            return output;
        }
        catch
        {
            return output;
        }
    }

    private static void InterpolateNaNs(double[] values)
    {
        var lastPointIndex = 0;

        for (var j = 0; j < values.Length; j++)
        {
            if (double.IsNaN(values[j]))
            {
                var firstPointIndex =
                    values.ToList().FindIndex(
                        j,
                        item => !double.IsNaN(item));

                if (firstPointIndex > 0)
                {
                    values[j] = LinearInterpolate(
                        j,
                        lastPointIndex,
                        firstPointIndex,
                        values[lastPointIndex],
                        values[firstPointIndex]);
                }
                else
                {
                    values[j] = values[lastPointIndex];
                }
            }
            else
            {
                lastPointIndex = j;
            }
        }
    }

    private static double LinearInterpolate(double x, double xa, double xb, double ya, double yb)
    {
        if (Math.Abs(xb - xa) < double.Epsilon)
            return ya;

        return ya + (yb - ya) * ((x - xa) / (xb - xa));
    }

    private static void RemoveAverage(double[] values)
    {
        if (values.Length == 0)
            return;

        var average = values.Average();
        for (var i = 0; i < values.Length; i++)
            values[i] -= average;
    }

    private static double[] Downsample(double[] values, int factor)
    {
        if (factor <= 1)
            return values.ToArray();

        var length = values.Length / factor;
        if (length == 0)
            return Array.Empty<double>();

        var result = new double[length];
        for (var i = 0; i < length; i++)
            result[i] = values[i * factor];

        return result;
    }

    private static int IndexOfMax(IReadOnlyList<double> values)
    {
        var maxIndex = 0;
        var maxValue = values[0];

        for (var i = 1; i < values.Count; i++)
        {
            if (values[i] > maxValue)
            {
                maxValue = values[i];
                maxIndex = i;
            }
        }

        return maxIndex;
    }

    private static string BuildSeriesName(RowsCacheSeries series)
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(series.IdName)) parts.Add(series.IdName.Trim());
        if (!string.IsNullOrWhiteSpace(series.Quantity)) parts.Add(series.Quantity.Trim().ToUpperInvariant());
        if (!string.IsNullOrWhiteSpace(series.Component)) parts.Add(series.Component.Trim().ToUpperInvariant());
        if (!string.IsNullOrWhiteSpace(series.Phase)) parts.Add(series.Phase.Trim().ToUpperInvariant());
        return string.Join('|', parts);
    }

    private sealed record WindowModalResult(
        Complex[] EigenValues,
        Complex[] ContinuousPoles,
        double[] Frequencies,
        double[] Damping,
        Matrix<Complex> Residues,
        double[] PseudoEnergy,
        double[] Idm,
        Matrix<Complex> EigenVectors,
        Matrix<Complex> OutputMatrix,
        int MaxEnergyIndex,
        int MaxIdmIndex);
}

public sealed record CcaParameters(
    int ModelOrder,
    int BlockRows,
    int WindowLengthMinutes,
    int WindowStepSeconds,
    double FrequencyMinHz,
    double FrequencyMaxHz);

public sealed class CcaComputeResult
{
    public IReadOnlyList<Cca.WindowPoint> Windows { get; init; } = Array.Empty<Cca.WindowPoint>();
    public DateTime FromUtc { get; init; }
    public DateTime ToUtc { get; init; }
    public CcaParameters Parameters { get; init; } = new(8, 20, 10, 60, 0.3, 0.4);
}