using Data.Sql;
using OpenPlot.Data.Dtos;
using Xunit;

namespace OpenPlot.Api.Tests;

public class RunsEndpointsTests
{
    [Theory]
    [InlineData("queued")]
    [InlineData("running")]
    [InlineData("absent")]
    [InlineData("failed")]
    [InlineData("done")]
    public void CreateSearchRunItem_ShouldPreserveConvComtrade(string convComtrade)
    {
        var runId = Guid.NewGuid();
        var row = new SearchRunRow
        {
            id = runId,
            status = "done",
            shared = true,
            owner = false,
            conv_comtrade = convComtrade
        };

        var item = RunsEndpoints.CreateSearchRunItem(row, "consulta_teste");

        Assert.Equal("consulta_teste", item.label);
        Assert.Equal("done", item.status);
        Assert.Equal(runId.ToString(), item.id);
        Assert.True(item.shared);
        Assert.False(item.owner);
        Assert.Equal(convComtrade, item.conv_comtrade);
    }

    [Fact]
    public void SearchRunRow_ShouldDefaultConvComtradeToAbsent()
    {
        var row = new SearchRunRow();

        Assert.Equal("absent", row.conv_comtrade);
    }

    [Fact]
    public void SearchRunItem_ShouldDefaultConvComtradeToAbsent()
    {
        var item = new ConfigEndpoints.SearchRunItem();

        Assert.Equal("absent", item.conv_comtrade);
    }

    [Fact]
    public void ListRunsSql_ShouldProjectConvComtradeFromComtradeRuns()
    {
        Assert.Contains("LEFT JOIN openplot.comtrade_runs AS c", SearchSql.ListRuns, StringComparison.Ordinal);
        Assert.Contains("WHEN c.run_id IS NULL THEN 'absent'", SearchSql.ListRuns, StringComparison.Ordinal);
        Assert.Contains("END AS conv_comtrade", SearchSql.ListRuns, StringComparison.Ordinal);
    }
}
