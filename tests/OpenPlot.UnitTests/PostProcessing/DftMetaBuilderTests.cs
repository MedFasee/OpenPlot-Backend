using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;
using Xunit;

namespace OpenPlot.UnitTests.PostProcessing;

public sealed class DftMetaBuilderTests
{
    [Fact]
    public void Build_WhenPayloadHasNoSeries_ReturnsFallbackMetadata()
    {
        var sut = new DftMetaBuilder(new PlotMetaBuilder());
        var payload = new RowsCacheV2
        {
            From = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            To = new DateTime(2025, 1, 1, 1, 0, 0, DateTimeKind.Utc),
            SelectRate = 60
        };

        var meta = sut.Build(payload);

        Assert.Equal("Espectro de Freq.", meta.Title);
        Assert.Equal("Tempo (UTC)", meta.XLabel);
        Assert.Contains("Freq", meta.YLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_WhenSeriesRepresentsPositiveSequence_ComposesSpectrumTitleFromPlotMetadata()
    {
        var sut = new DftMetaBuilder(new PlotMetaBuilder());
        var payload = new RowsCacheV2
        {
            From = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            To = new DateTime(2025, 1, 1, 0, 10, 0, DateTimeKind.Utc),
            SelectRate = 60,
            Series =
            [
                new RowsCacheSeries
                {
                    IdName = "PMU-1",
                    PdcName = "PDC-1",
                    Quantity = "voltage",
                    Component = "seq",
                    Phase = "pos",
                    Unit = "V",
                    Points =
                    [
                        new RowsCachePoint { Ts = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), Value = 10 },
                        new RowsCachePoint { Ts = new DateTime(2025, 1, 1, 0, 0, 1, DateTimeKind.Utc), Value = 11 }
                    ]
                }
            ]
        };

        var meta = sut.Build(payload);

        Assert.Equal("Espectro de Freq. do Módulo da Tensão - Sequência Positiva - 60 fasores/s", meta.Title);
        Assert.Equal("Tempo (UTC) - Dia 01/01/2025", meta.XLabel);
        Assert.Contains("Freq", meta.YLabel, StringComparison.OrdinalIgnoreCase);
    }
}
