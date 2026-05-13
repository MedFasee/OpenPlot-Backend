using OpenPlot.Data.Dtos;

namespace OpenPlot.Features.Export;

public sealed record ExportArtifactDescriptor(string? DirectoryPath, string? FileName)
{
    public bool IsMissing => string.IsNullOrWhiteSpace(DirectoryPath) || string.IsNullOrWhiteSpace(FileName);

    public string FullPath => Path.Combine(DirectoryPath ?? string.Empty, FileName ?? string.Empty);

    public static ExportArtifactDescriptor FromStatusRow(ExportRunStatusRow row)
        => new(row.dir_path, row.file_name);
}

public interface IExportArtifactStore
{
    IResult ResolveDownloadResult(ExportArtifactDescriptor artifact);
    void DeleteIfExists(ExportArtifactDescriptor artifact);
}

public sealed class DiskExportArtifactStore : IExportArtifactStore
{
    public IResult ResolveDownloadResult(ExportArtifactDescriptor artifact)
    {
        if (artifact.IsMissing)
            return Results.NotFound("arquivo de exportação não localizado.");

        if (!File.Exists(artifact.FullPath))
            return Results.NotFound("arquivo de exportação não encontrado em disco.");

        var contentType = string.Equals(Path.GetExtension(artifact.FileName), ".zip", StringComparison.OrdinalIgnoreCase)
            ? "application/zip"
            : "application/octet-stream";

        return Results.File(artifact.FullPath, contentType, artifact.FileName);
    }

    public void DeleteIfExists(ExportArtifactDescriptor artifact)
    {
        if (artifact.IsMissing)
            return;

        try
        {
            File.Delete(artifact.FullPath);

            if (Directory.Exists(artifact.DirectoryPath) && !Directory.EnumerateFileSystemEntries(artifact.DirectoryPath).Any())
                Directory.Delete(artifact.DirectoryPath);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
