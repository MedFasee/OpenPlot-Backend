using System.Security.Cryptography;
using System.Text;
using OpenPlot.ExportWorker.Storage;

namespace OpenPlot.UnitTests.Export;

public sealed class DiskExportStoreTests
{
    [Fact]
    public void ResolveRunZipPath_BuildsDailyFolderAndSafeFileName()
    {
        var sut = new DiskExportStore();
        var runId = Guid.Parse("11111111-2222-3333-4444-555555555555");

        var result = sut.ResolveRunZipPath("exports", runId, "PMU: Norte/01");

        Assert.EndsWith(Path.Combine("comtrade", DateTimeOffset.Now.ToString("yyyy-MM-dd")), result.DirPath);
        Assert.StartsWith("comtrade__", result.FileName);
        Assert.EndsWith($"__{runId:N}.zip", result.FileName);
        Assert.DoesNotContain(':', result.FileName);
    }

    [Fact]
    public async Task WriteZipAtomicallyAsync_WritesFileAndReturnsDeterministicHash()
    {
        var sut = new DiskExportStore();
        var root = Path.Combine(Path.GetTempPath(), "openplot-tests", Guid.NewGuid().ToString("N"));
        var bytes = Encoding.UTF8.GetBytes("openplot-export");
        var expectedSha = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

        try
        {
            var result = await sut.WriteZipAtomicallyAsync(
                root,
                "sample.zip",
                stream => stream.Write(bytes, 0, bytes.Length),
                CancellationToken.None);

            var fullPath = Path.Combine(root, "sample.zip");

            Assert.True(File.Exists(fullPath));
            Assert.False(File.Exists(fullPath + ".tmp"));
            Assert.Equal(bytes.Length, result.SizeBytes);
            Assert.Equal(expectedSha, result.Sha256);
            Assert.Equal(bytes, await File.ReadAllBytesAsync(fullPath));
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }
}
