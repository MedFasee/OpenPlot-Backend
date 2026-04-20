using System.Text.Json;

namespace OpenPlot.Api.IntegrationTests.Export;

public sealed class ExportEndpointsValidationTests
{
    [Theory]
    [InlineData("done")]
    [InlineData("DONE")]
    public void CanConvertSearchRun_WhenStatusIsDone_ReturnsTrue(string status)
    {
        Assert.True(ExportEndpoints.CanConvertSearchRun(status));
    }

    [Theory]
    [InlineData("queued")]
    [InlineData("running")]
    [InlineData("failed")]
    [InlineData("completed")]
    [InlineData(null)]
    public void CanConvertSearchRun_WhenStatusIsNotDone_ReturnsFalse(string? status)
    {
        Assert.False(ExportEndpoints.CanConvertSearchRun(status));
    }

    [Fact]
    public void BuildIncompleteRunError_ReturnsExpectedMessageAndStatus()
    {
        var payload = ExportEndpoints.BuildIncompleteRunError("running");
        var json = JsonSerializer.SerializeToDocument(payload);

        Assert.Equal(
            "A consulta não está concluída. Só é possível converter consultas completas/íntegras.",
            json.RootElement.GetProperty("error").GetString());
        Assert.Equal("running", json.RootElement.GetProperty("status").GetString());
    }
}
