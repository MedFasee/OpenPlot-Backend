using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.UnitTests.PostProcessing;

public sealed class CcaTests
{
    [Fact]
    public void Compute_WhenPayloadIsValid_ReturnsWindowsEnergyAndIdm()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 240, seriesCount: 2, frequencyHz: 0.35);

        var result = Cca.Compute(
            payload,
            modelOrder: 4,
            blockRows: 10,
            windowLengthMinutes: 3,
            windowStepSeconds: 30,
            frequencyMinHz: 0.3,
            frequencyMaxHz: 0.4);

        Assert.Equal(start, result.FromUtc);
        Assert.Equal(payload.To, result.ToUtc);
        Assert.NotEmpty(result.Windows);
        Assert.Equal(4, result.Parameters.ModelOrder);

        var firstWindow = result.Windows[0];
        Assert.True(firstWindow.Energy.Vector.Count == 2);
        Assert.True(firstWindow.Idm.Vector.Count == 2);
        Assert.InRange(firstWindow.Energy.FrequencyHz, 0.2, 0.5);
        Assert.InRange(firstWindow.Idm.FrequencyHz, 0.2, 0.5);
        Assert.True(firstWindow.AllModes.Count == 4);
    }

    [Fact]
    public void Compute_WhenWindowHasTooFewPointsForBlockRows_ThrowsInvalidOperationException()
    {
        var payload = CreateOscillatoryPayload(
            new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            sampleRate: 1,
            sampleCount: 120,
            seriesCount: 1,
            frequencyHz: 0.35);

        var ex = Assert.Throws<InvalidOperationException>(() => Cca.Compute(
            payload,
            modelOrder: 4,
            blockRows: 100,
            windowLengthMinutes: 1,
            windowStepSeconds: 10,
            frequencyMinHz: 0.3,
            frequencyMaxHz: 0.4));

        Assert.Contains("linhas por bloco", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Compute_WhenWindowLengthExceedsSignalLength_ThrowsInvalidOperationException()
    {
        var payload = CreateOscillatoryPayload(
            new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            sampleRate: 1,
            sampleCount: 120,
            seriesCount: 1,
            frequencyHz: 0.35);

        var ex = Assert.Throws<InvalidOperationException>(() => Cca.Compute(
            payload,
            modelOrder: 4,
            blockRows: 10,
            windowLengthMinutes: 3,
            windowStepSeconds: 10,
            frequencyMinHz: 0.3,
            frequencyMaxHz: 0.4));

        Assert.Contains("janela deslizante", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static RowsCacheV2 CreateOscillatoryPayload(
        DateTime start,
        int sampleRate,
        int sampleCount,
        int seriesCount,
        double frequencyHz)
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
                    Value = Math.Cos(2.0 * Math.PI * frequencyHz * t + (seriesIndex * Math.PI / 8.0))
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
