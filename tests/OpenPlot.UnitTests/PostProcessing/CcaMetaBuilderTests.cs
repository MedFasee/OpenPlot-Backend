using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.UnitTests.PostProcessing;

public sealed class CcaMetaBuilderTests
{
    [Fact]
    public void Build_WhenPayloadHasNoSeries_ReturnsFallbackMetadata()
    {
        var sut = new CcaMetaBuilder(new PlotMetaBuilder());
        var payload = new RowsCacheV2
        {
            From = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            To = new DateTime(2025, 1, 1, 1, 0, 0, DateTimeKind.Utc),
            SelectRate = 60
        };

        var meta = sut.Build(payload);

        Assert.Equal("CCA", meta.Title);
        Assert.Equal("Tempo (UTC)", meta.XLabel);
        Assert.Equal("Frequência (Hz)", meta.FrequencyYLabel);
        Assert.Equal("Amortecimento (%)", meta.DampingYLabel);
        Assert.Equal("Pseudoenergia", meta.EnergyYLabel);
        Assert.Equal("IDM", meta.IdmYLabel);
    }

    [Fact]
    public void Build_WhenSeriesRepresentsPositiveSequence_ComposesCcaTitleFromPlotMetadata()
    {
        var sut = new CcaMetaBuilder(new PlotMetaBuilder());
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

        Assert.Equal("CCA do Módulo da Tensão - Sequência Positiva - 60 fasores/s", meta.Title);
        Assert.Equal("Tempo (UTC) - Dia 01/01/2025", meta.XLabel);
        Assert.Equal("Frequência (Hz)", meta.FrequencyYLabel);
        Assert.Equal("Amortecimento (%)", meta.DampingYLabel);
    }
}
