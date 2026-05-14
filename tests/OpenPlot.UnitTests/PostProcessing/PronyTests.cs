using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.UnitTests.PostProcessing;

public sealed class PronyTests
{
    [Fact]
    public void Compute_WhenPayloadIsValid_ReturnsSpecsCandidatesAndTimeSeries()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 20, sampleCount: 40);

        var result = Prony.Compute(payload, order: 4);

        Assert.Equal(start, result.FromUtc);
        Assert.Equal(start.AddSeconds((40 - 1) / 20d), result.ToUtc);
        Assert.NotEmpty(result.Specs);
        Assert.Empty(result.ModeShapeCandidatesHz);
        Assert.Equal(result.ModeShapeCandidatesHz.OrderBy(x => x.FrequencyHz), result.ModeShapeCandidatesHz);

        var first = Assert.Single(result.Specs.Values);
        Assert.Equal(20, first.Sr, precision: 10);
        Assert.Equal(40, first.N);
        Assert.Equal(4, first.Order);
        Assert.Equal("PMU-1", first.Pmu);
        Assert.Equal("frequency", first.Quantity);
        Assert.Equal("freq", first.Component);
        Assert.Equal("Hz", first.Unit);
        Assert.Equal(first.N, first.OriginalPoints.Count);
        Assert.Equal(first.N, first.EstimatedPoints.Count);
        Assert.Equal(4, first.AllModes.Count);
        Assert.True(first.Modes.Count <= first.AllModes.Count);

        var expectedModeOrder = first.AllModes
            .Where(m => m.FrequencyHz < 10.0 && m.FrequencyHz > 1e-6 && m.Energy > 1e-3)
            .OrderByDescending(m => m.Energy)
            .Select(m => m.Index)
            .ToArray();

        Assert.Equal(expectedModeOrder, first.Modes.Select(m => m.Index).ToArray());
    }

    [Fact]
    public void Compute_WhenPayloadHasAtLeastTwoValidSeries_ReturnsModeShapeCandidates()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 20, sampleCount: 40, seriesCount: 2);

        var result = Prony.Compute(payload, order: 4);

        Assert.NotEmpty(result.ModeShapeCandidatesHz);
        Assert.Equal(result.ModeShapeCandidatesHz.OrderBy(x => x.FrequencyHz), result.ModeShapeCandidatesHz);
        Assert.All(result.ModeShapeCandidatesHz, candidate => Assert.InRange(candidate.FrequencyHz, 0.000001, 10.0));
        Assert.Contains(result.ModeShapeCandidatesHz, candidate => Math.Abs(candidate.FrequencyHz - 1.0) < 0.25);
        Assert.All(result.ModeShapeCandidatesHz, candidate => Assert.Equal(2, candidate.Vector.Count));
        Assert.All(result.ModeShapeCandidatesHz.SelectMany(candidate => candidate.Vector), point =>
        {
            Assert.False(string.IsNullOrWhiteSpace(point.Series));
            Assert.False(string.IsNullOrWhiteSpace(point.Pmu));
        });

        var first = result.Specs.Values.First();
        var expectedCandidates = first.AllModes
            .Where(m => m.FrequencyHz < 10.0 && m.FrequencyHz > 1e-6)
            .Select(m => m.Index)
            .ToArray();

        var expectedFrequencies = first.AllModes
            .Select(m => m.FrequencyHz)
            .Where(f => f < 10.0 && f > 1e-6)
            .OrderBy(f => f)
            .ToArray();

        Assert.Equal(expectedFrequencies, result.ModeShapeCandidatesHz.Select(candidate => candidate.FrequencyHz).ToArray());
        Assert.Equal(expectedCandidates, result.ModeShapeCandidatesHz.OrderBy(candidate => candidate.FrequencyHz).Select(candidate => candidate.Index).ToArray());
    }

    [Fact]
    public void Compute_WhenOrderIsZero_ThrowsArgumentOutOfRangeException()
    {
        var payload = CreateOscillatoryPayload(new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), sampleRate: 10, sampleCount: 10);

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => Prony.Compute(payload, order: 0));

        Assert.Contains("ordem", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Compute_WhenWindowIsInvalid_ThrowsInvalidOperationException()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 10, sampleCount: 10);

        var ex = Assert.Throws<InvalidOperationException>(() => Prony.Compute(
            payload,
            order: 2,
            fromUtc: start.AddSeconds(5),
            toUtc: start.AddSeconds(2)));

        Assert.Contains("Janela inválida", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Compute_WhenOrderIsGreaterThanRoundedQuarterOfSampleCount_ThrowsInvalidOperationException()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 313);

        var ex = Assert.Throws<InvalidOperationException>(() => Prony.Compute(payload, order: 79));

        Assert.Contains("Ordem inválida", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("79", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("78", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Compute_WhenWindowHasTooFewSamplesForRequestedOrder_ThrowsInvalidOperationException()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 10, sampleCount: 5, seriesCount: 2);

        var ex = Assert.Throws<InvalidOperationException>(() => Prony.Compute(payload, order: 1));

        Assert.Contains("poucas amostras", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static RowsCacheV2 CreateOscillatoryPayload(DateTime start, int sampleRate, int sampleCount, int seriesCount = 1)
    {
        var series = new List<RowsCacheSeries>(seriesCount);

        for (var seriesIndex = 0; seriesIndex < seriesCount; seriesIndex++)
        {
            var points = new List<RowsCachePoint>(sampleCount);
            for (var i = 0; i < sampleCount; i++)
            {
                var t = i / (double)sampleRate;
                points.Add(new RowsCachePoint
                {
                    Ts = start.AddSeconds(t),
                    Value = Math.Cos(2.0 * Math.PI * 1.0 * t + (seriesIndex * Math.PI / 6.0))
                });
            }

            series.Add(new RowsCacheSeries
            {
                IdName = $"PMU-{seriesIndex + 1}",
                PdcName = "PDC-1",
                Quantity = "frequency",
                Component = "freq",
                Unit = "Hz",
                Phase = ((char)('A' + seriesIndex)).ToString(),
                Points = points
            });
        }

        return new RowsCacheV2
        {
            From = start,
            To = series[0].Points[^1].Ts,
            SelectRate = sampleRate,
            Series = series
        };
    }
}
