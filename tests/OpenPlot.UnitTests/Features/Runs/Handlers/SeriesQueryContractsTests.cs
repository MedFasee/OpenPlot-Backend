using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Handlers.Abstractions;

namespace OpenPlot.UnitTests.Features.Runs.Handlers;

public sealed class SeriesQueryContractsTests
{
    private sealed class FakeSeriesQuery : ISeriesQuery
    {
        public Guid RunId { get; init; } = Guid.NewGuid();
        public string? MaxPoints { get; init; }

        public bool MaxPointsIsAll =>
            string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

        public int ResolveMaxPoints(int @default = 5000)
        {
            if (MaxPointsIsAll)
            {
                return int.MaxValue;
            }

            if (string.IsNullOrWhiteSpace(MaxPoints))
            {
                return @default;
            }

            return int.TryParse(MaxPoints, out var parsed) && parsed > 0
                ? parsed
                : @default;
        }
    }

    [Theory]
    [InlineData("all", int.MaxValue, true)]
    [InlineData("ALL", int.MaxValue, true)]
    [InlineData("2500", 2500, false)]
    [InlineData("invalid", 5000, false)]
    [InlineData("-100", 5000, false)]
    public void ResolveMaxPoints_WhenInputVaries_ReturnsExpectedValue(string maxPoints, int expected, bool maxPointsIsAll)
    {
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = maxPoints };

        var resolved = query.ResolveMaxPoints();

        Assert.Equal(expected, resolved);
        Assert.Equal(maxPointsIsAll, query.MaxPointsIsAll);
    }

    [Fact]
    public void ResolveMaxPoints_WhenValueIsNull_UsesProvidedDefault()
    {
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = null };

        var resolved = query.ResolveMaxPoints(@default: 7500);

        Assert.Equal(7500, resolved);
    }

    [Fact]
    public void RunId_WhenAssigned_IsPreserved()
    {
        var runId = Guid.NewGuid();
        ISeriesQuery query = new FakeSeriesQuery { RunId = runId };

        Assert.Equal(runId, query.RunId);
    }

    [Theory]
    [InlineData("5000", 5000, false)]
    [InlineData("all", int.MaxValue, true)]
    [InlineData("ALL", int.MaxValue, true)]
    [InlineData("invalid", 1200, false)]
    public void AngleDiffQuery_ResolveMaxPoints_WhenInputVaries_ReturnsExpectedValue(string maxPoints, int expected, bool maxPointsIsAll)
    {
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            MaxPoints = maxPoints
        };

        var resolved = query.ResolveMaxPoints(@default: 1200);

        Assert.Equal(expected, resolved);
        Assert.Equal(maxPointsIsAll, query.MaxPointsIsAll);
    }

    [Theory]
    [InlineData("voltage")]
    [InlineData("current")]
    [InlineData("VOLTAGE")]
    [InlineData("CURRENT")]
    public void AngleDiffQuery_WhenKindIsAssigned_PreservesValue(string kind)
    {
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            Kind = kind,
            Reference = "PMU_REF"
        };

        Assert.Equal(kind, query.Kind);
        Assert.Equal("PMU_REF", query.Reference);
    }

    [Theory]
    [InlineData("A")]
    [InlineData("B")]
    [InlineData("C")]
    public void AngleDiffQuery_WhenPhaseIsAssigned_PreservesValue(string phase)
    {
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            Phase = phase
        };

        Assert.Equal(phase, query.Phase);
    }

    [Theory]
    [InlineData("pos")]
    [InlineData("neg")]
    [InlineData("zero")]
    [InlineData("seq+")]
    [InlineData("seq-")]
    [InlineData("seq0")]
    public void AngleDiffQuery_WhenSequenceIsAssigned_PreservesValue(string sequence)
    {
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            Sequence = sequence
        };

        Assert.Equal(sequence, query.Sequence);
    }

    [Fact]
    public void ByRunQuery_WhenTriModeIsEnabled_PreservesPmu()
    {
        var query = new ByRunQuery
        {
            RunId = Guid.NewGuid(),
            Tri = true,
            Pmu = "PMU1"
        };

        Assert.True(query.Tri);
        Assert.Equal("PMU1", query.Pmu);
    }

    [Fact]
    public void PowerPlotQuery_WhenTriAndTotalAreSet_PreservesBothFlags()
    {
        var query = new PowerPlotQuery
        {
            RunId = Guid.NewGuid(),
            Tri = true,
            Total = true,
            Which = "active"
        };

        Assert.True(query.Tri);
        Assert.True(query.Total);
        Assert.Equal("active", query.Which);
    }
}
