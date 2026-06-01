using System.Security.Cryptography;
using OpenPlot.ExportWorker.Comtrade;

namespace OpenPlot.ExportWorker.Storage;

public sealed record ExportArtifactPath(string DirPath, string FileName);
public sealed record ExportArtifactWriteResult(long SizeBytes, string Sha256);

public interface IExportArtifactStore
{
    ExportArtifactPath ResolveRunZipPath(string rootDir, Guid runId, string label);
    Task<ExportArtifactWriteResult> WriteZipAtomicallyAsync(
        string finalDir,
        string finalFileName,
        Action<Stream> writeToStream,
        CancellationToken ct);
}

public sealed class DiskExportStore
    : IExportArtifactStore
{
    public ExportArtifactPath ResolveRunZipPath(string rootDir, Guid runId, string label)
    {
        // organiza por data (hoje). Se você preferir por created_at do run, dá pra mudar.
        var day = DateTimeOffset.Now.ToString("yyyy-MM-dd");
        var dir = Path.Combine(rootDir, "comtrade", day);

        // nome do zip: label + runid (file-safe)
        var safeLabel = Naming.SafeFileBase(string.IsNullOrWhiteSpace(label) ? "run" : label, 60);
        var file = $"{safeLabel}.zip";

        return new ExportArtifactPath(dir, file);
    }

    public async Task<ExportArtifactWriteResult> WriteZipAtomicallyAsync(
        string finalDir,
        string finalFileName,
        Action<Stream> writeToStream,
        CancellationToken ct)
    {
        Directory.CreateDirectory(finalDir);

        var finalPath = Path.Combine(finalDir, finalFileName);
        var tmpPath = finalPath + ".tmp";

        if (File.Exists(tmpPath)) File.Delete(tmpPath);

        await using (var fs = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None))
        {
            writeToStream(fs);
            await fs.FlushAsync(ct);
        }

        string sha;
        await using (var fsHash = new FileStream(tmpPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            var hash = await SHA256.HashDataAsync(fsHash, ct);
            sha = Convert.ToHexString(hash).ToLowerInvariant();
        }

        var size = new FileInfo(tmpPath).Length;

        if (File.Exists(finalPath)) File.Delete(finalPath);
        File.Move(tmpPath, finalPath);

        return new ExportArtifactWriteResult(size, sha);
    }
}