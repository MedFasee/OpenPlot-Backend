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

        Assert.Equal("Tempo (UTC) - Dia 02/01/2025", meta.XLabel);
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

        Assert.Equal("Diferença Angular da Tensão Ref.: PMU-REF - 30 fasores/s", meta.Title);
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

        Assert.Equal("Distorção de Tensão Harmônica Total - Fase B - 120 fasores/s", meta.Title);
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
        Assert.Equal("Var. de Frequência (Hz/s)", meta.YLabel);
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
