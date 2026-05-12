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
        Assert.NotEmpty(result.ModeShapeCandidatesHz);
        Assert.Equal(result.ModeShapeCandidatesHz.OrderBy(x => x), result.ModeShapeCandidatesHz);
        Assert.All(result.ModeShapeCandidatesHz, candidate => Assert.InRange(candidate, 0.000001, 10.0));

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
        Assert.Contains(result.ModeShapeCandidatesHz, candidate => Math.Abs(candidate - 1.0) < 0.25);
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
    public void Compute_WhenOrderIsGreaterThanOrEqualToSampleCount_ThrowsInvalidOperationException()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 4);

        var ex = Assert.Throws<InvalidOperationException>(() => Prony.Compute(payload, order: 4));

        Assert.Contains("Ordem inválida", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Compute_WhenWindowHasTooFewSamplesForRequestedOrder_ThrowsInvalidOperationException()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 10, sampleCount: 5);

        var ex = Assert.Throws<InvalidOperationException>(() => Prony.Compute(payload, order: 3));

        Assert.Contains("poucas amostras", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static RowsCacheV2 CreateOscillatoryPayload(DateTime start, int sampleRate, int sampleCount)
    {
        var points = new List<RowsCachePoint>(sampleCount);
        for (var i = 0; i < sampleCount; i++)
        {
            var t = i / (double)sampleRate;
            points.Add(new RowsCachePoint
            {
                Ts = start.AddSeconds(t),
                Value = Math.Cos(2.0 * Math.PI * 1.0 * t)
            });
        }

        return new RowsCacheV2
        {
            From = start,
            To = points[^1].Ts,
            SelectRate = sampleRate,
            Series =
            [
                new RowsCacheSeries
                {
                    IdName = "PMU-1",
                    PdcName = "PDC-1",
                    Quantity = "frequency",
                    Component = "freq",
                    Unit = "Hz",
                    Points = points
                }
            ]
        };
    }
}
