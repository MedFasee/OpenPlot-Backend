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

    [Fact]
    public void IsExpiredExport_WhenFinishedAtIsOlderThanSevenDays_ReturnsTrue()
    {
        var nowUtc = new DateTimeOffset(2025, 1, 8, 12, 0, 0, TimeSpan.Zero);
        var finishedAt = nowUtc.AddDays(-8).UtcDateTime;

        var expired = ExportEndpoints.IsExpiredExport(finishedAt, nowUtc);

        Assert.True(expired);
    }

    [Fact]
    public void IsExpiredExport_WhenFinishedAtIsWithinSevenDays_ReturnsFalse()
    {
        var nowUtc = new DateTimeOffset(2025, 1, 8, 12, 0, 0, TimeSpan.Zero);
        var finishedAt = nowUtc.AddDays(-6).UtcDateTime;

        var expired = ExportEndpoints.IsExpiredExport(finishedAt, nowUtc);

        Assert.False(expired);
    }

    [Fact]
    public void DeleteExpiredExportFile_WhenFileWasCreatedMoreThanSevenDaysAgo_DeletesFileAndEmptyDirectory()
    {
        var rootDir = Path.Combine(Path.GetTempPath(), "openplot-export-tests", Guid.NewGuid().ToString("N"));
        var dirPath = Path.Combine(rootDir, "comtrade", "2025-01-01");
        var fileName = "expired-export.zip";
        var fullPath = Path.Combine(dirPath, fileName);

        Directory.CreateDirectory(dirPath);
        File.WriteAllText(fullPath, "expired export");

        var oldTimestamp = DateTime.UtcNow.AddDays(-8);
        File.SetCreationTimeUtc(fullPath, oldTimestamp);
        File.SetLastWriteTimeUtc(fullPath, oldTimestamp);

        ExportEndpoints.DeleteExpiredExportFile(dirPath, fileName);

        Assert.False(File.Exists(fullPath));
        Assert.False(Directory.Exists(dirPath));

        if (Directory.Exists(rootDir))
            Directory.Delete(rootDir, recursive: true);
    }
}
