using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.UnitTests.PostProcessing;

public sealed class PlotMetaBuilderTests
{
    private static readonly PlotMetaBuilder Sut = new();

    [Fact]
    public void Build_WhenSameDay_UsesDateInXAxisLabel()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 60);
        var meas = new MeasurementsQuery("frequency", "freq", Unit: "Hz");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Tempo (UTC) - Data 02/01/2025", meta.XLabel);
        Assert.Equal("Frequência - 60 fasores/s", meta.Title);
        Assert.Equal("Frequência (Hz)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenAngleDifference_UsesReferenceTerminalInTitle()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 30);
        var meas = new MeasurementsQuery(
            Quantity: "voltage",
            Component: "angle",
            PhaseMode: PhaseMode.Single,
            Phase: "A",
            Unit: "deg",
            ReferenceTerminal: "PMU-REF");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Diferença Angular da Tensão - Fase A - Ref.: PMU-REF - 30 fasores/s", meta.Title);
        Assert.Equal("Diferença Angular (Graus)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenThdSinglePhase_UsesPhaseInTitle()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 120);
        var meas = new MeasurementsQuery(
            Quantity: "voltage",
            Component: "thd",
            PhaseMode: PhaseMode.Single,
            Phase: "B",
            Unit: "%");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Distorção Harmônica Total de Tensão - Fase B - 120 fasores/s", meta.Title);
        Assert.Equal("Distorção Harmônica (%)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenDfreq_UsesSpecificYAxisLabel()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 3, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 1);
        var meas = new MeasurementsQuery("frequency", "dfreq", Unit: "Hz/s");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Tempo (UTC)", meta.XLabel);
        Assert.Equal("Variação de Frequência - 1 fasor/s", meta.Title);
        Assert.Equal("Variação de Frequência (Hz/s)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenCurrentMagnitudeIsThreePhase_UsesTerminalAndPerUnitYAxisLabel()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 60);
        var meas = new MeasurementsQuery("current", "mag", PhaseMode.ThreePhase, PmuNames: ["PMU-1"], Unit: "pu");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Módulo da Corrente - PMU-1 - 60 fasores/s", meta.Title);
        Assert.Equal("Corrente (pu)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenVoltageAngleIsPositiveSequence_UsesSequenceAndReferenceTerminal()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 100);
        var meas = new MeasurementsQuery("voltage", "angle", PhaseMode.SeqPos, ReferenceTerminal: "PMU-REF", Unit: "deg");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Diferença Angular da Tensão - Sequência Positiva - Ref.: PMU-REF - 100 fasores/s", meta.Title);
        Assert.Equal("Diferença Angular (Graus)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenVoltageMagnitudeIsSinglePhase_UsesPhaseInTitle()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 60);
        var meas = new MeasurementsQuery("voltage", "mag", PhaseMode.Single, Phase: "A", Unit: "raw");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Módulo da Tensão - Fase A - 60 fasores/s", meta.Title);
        Assert.Equal("Tensão (V)", meta.YLabel);
    }

    [Fact]
    public void Build_WhenDigitalSignal_UsesDigitalLabelsAndSampleRate()
    {
        var from = new DateTime(2025, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2025, 1, 2, 11, 0, 0, DateTimeKind.Utc);
        var ctx = CreateRunContext(from, to, selectRate: 1);
        var meas = new MeasurementsQuery("digital", "dig");

        var meta = Sut.Build(new WindowQuery(from, to), ctx, meas);

        Assert.Equal("Sinal Digital de Falha de Comutação - 1 amostra/s", meta.Title);
        Assert.Equal("Sinal Digital de Falha de Comutação (Binário)", meta.YLabel);
    }

    private static RunContext CreateRunContext(DateTime from, DateTime to, int selectRate)
        => new(
            RunId: Guid.NewGuid(),
            PdcName: "PDC-1",
            FromUtc: from,
            ToUtc: to,
            PdcId: 10,
            PmuNames: ["PMU-1"],
            SelectRate: selectRate);
}
