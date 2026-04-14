using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.UnitTests.PostProcessing;

public sealed class DftTests
{
    [Fact]
    public void CalculateZoomBounds_WhenSamplingRateIsOne_UsesFMinAsPosition()
    {
        var zoom = Dft.CalculateZoomBounds(ndat: 8, sr: 1);

        Assert.Equal(0.25, zoom.Position, precision: 10);
        Assert.Equal(0.5, zoom.Size, precision: 10);
    }

    [Fact]
    public void CalculateZoomBounds_WhenSamplingRateIsGreaterThanOne_UsesZeroAsPosition()
    {
        var zoom = Dft.CalculateZoomBounds(ndat: 12, sr: 60);

        Assert.Equal(0, zoom.Position, precision: 10);
        Assert.Equal(1.6, zoom.Size, precision: 10);
    }

    [Fact]
    public void ResampleHoldLast_FillsMissingSlotsWithLastKnownValue()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var raw = new[]
        {
            new Point(start, 10),
            new Point(start.AddSeconds(2), 20),
            new Point(start.AddSeconds(3), 30)
        };

        var result = Dft.ResampleHoldLast(raw, sr: 1);

        Assert.Equal(new[] { 10d, 10d, 20d, 30d }, result);
    }

    [Fact]
    public void ForwardSingleSided_ReturnsHalfSpectrumAndZeroMagnitudeAtDc()
    {
        var spec = Dft.ForwardSingleSided(new[] { 1d, 1d, 1d, 1d }, sr: 4);

        Assert.Equal(4, spec.Sr, precision: 10);
        Assert.Equal(4, spec.N);
        Assert.Equal(2, spec.FMin, precision: 10);
        Assert.Equal(3, spec.Points.Count);
        Assert.Equal(0, spec.Points[0].Mag, precision: 10);
        Assert.Equal(0, spec.Points[0].Hz, precision: 10);
        Assert.Equal(1, spec.Points[1].Hz, precision: 10);
        Assert.Equal(2, spec.Points[2].Hz, precision: 10);
    }

    [Fact]
    public void Compute_FiltersByWindowAndBuildsSeriesKeyWithMetadata()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var payload = new RowsCacheV2
        {
            From = start,
            To = start.AddSeconds(3),
            SelectRate = 1,
            Series =
            [
                new RowsCacheSeries
                {
                    IdName = "PMU-1",
                    PdcName = "PDC-1",
                    Quantity = "voltage",
                    Component = "mag",
                    Phase = "A",
                    Unit = "V",
                    Points =
                    [
                        new RowsCachePoint { Ts = start, Value = 1 },
                        new RowsCachePoint { Ts = start.AddSeconds(1), Value = 2 },
                        new RowsCachePoint { Ts = start.AddSeconds(2), Value = 3 },
                        new RowsCachePoint { Ts = start.AddSeconds(3), Value = 4 }
                    ]
                }
            ]
        };

        var result = Dft.Compute(payload, start.AddSeconds(1), start.AddSeconds(2));

        Assert.Equal(start.AddSeconds(1), result.FromUtc);
        Assert.Equal(start.AddSeconds(2), result.ToUtc);
        var entry = Assert.Single(result.Specs);
        Assert.Equal("PMU-1|VOLTAGE|MAG|A", entry.Key);
        Assert.Equal("PMU-1", entry.Value.Pmu);
        Assert.Equal("A", entry.Value.Phase);
        Assert.Equal("mag", entry.Value.Component);
        Assert.Equal("voltage", entry.Value.Quantity);
        Assert.Equal("V", entry.Value.Unit);
        Assert.Equal(2, entry.Value.N);
        Assert.NotNull(result.Zoom);
    }
}
