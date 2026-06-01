using System.Data;
using Dapper;
using Data.Sql;
using OpenPlot.Data.Dtos;

namespace OpenPlot.Features.Export;

public interface IExportFileService
{
    Task PurgeExpiredExportsAsync(IDbConnection db, CancellationToken ct);
    IResult ResolveFileResult(ExportRunStatusRow row);
}

public sealed class ExportFileService : IExportFileService
{
    private readonly IExportArtifactStore _artifactStore;

    public ExportFileService(IExportArtifactStore artifactStore)
    {
        _artifactStore = artifactStore;
    }

    public async Task PurgeExpiredExportsAsync(IDbConnection db, CancellationToken ct)
    {
        var expiredRows = await db.QueryAsync<ExpiredExportFileRow>(
            new CommandDefinition(ExportSql.DeleteExpiredExportRuns, cancellationToken: ct));

        foreach (var row in expiredRows)
            _artifactStore.DeleteIfExists(new ExportArtifactDescriptor(row.dir_path, row.file_name));
    }

    public IResult ResolveFileResult(ExportRunStatusRow row)
        => _artifactStore.ResolveDownloadResult(ExportArtifactDescriptor.FromStatusRow(row));
}
