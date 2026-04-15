using Dapper;
using OpenPlot.Features.Runs.Handlers;

namespace OpenPlot.UnitTests.Features.Runs.Handlers;

public sealed class PmuQueryHelperTests
{
    private static readonly PmuQueryHelper Sut = new();

    [Fact]
    public void Normalize_WhenSourcesContainNullsSpacesAndDuplicates_ReturnsDistinctTrimmedValues()
    {
        var result = Sut.Normalize(
            new string?[] { " PMU1 ", null, "PMU2" },
            new string?[] { "pmu1", string.Empty, "PMU3" });

        Assert.Equal(["PMU1", "PMU2", "PMU3"], result);
    }

    [Fact]
    public void NormalizeExcluding_WhenExcludedValueExists_RemovesItIgnoringCase()
    {
        var result = Sut.NormalizeExcluding("pmu2", new string?[] { "PMU1", "PMU2", "PMU3" });

        Assert.Equal(["PMU1", "PMU3"], result);
    }

    [Fact]
    public void BuildOrSqlFilter_WhenPmuListIsEmpty_ReturnsTrue()
    {
        var filter = Sut.BuildOrSqlFilter("c.id_name", []);

        Assert.Equal("TRUE", filter);
    }

    [Fact]
    public void BuildOrSqlFilter_WhenPmuListHasValues_ReturnsParameterizedOrExpression()
    {
        var filter = Sut.BuildOrSqlFilter("c.id_name", ["PMU1", "PMU2"]);

        Assert.Equal("LOWER(c.id_name) = LOWER(@pmu0) OR LOWER(c.id_name) = LOWER(@pmu1)", filter);
    }

    [Fact]
    public void AddSqlParameters_WhenPmuListHasValues_AddsIndexedParameters()
    {
        var parameters = new DynamicParameters();

        Sut.AddSqlParameters(parameters, ["PMU1", "PMU2"]);

        Assert.Equal("PMU1", parameters.Get<string>("pmu0"));
        Assert.Equal("PMU2", parameters.Get<string>("pmu1"));
    }
}
