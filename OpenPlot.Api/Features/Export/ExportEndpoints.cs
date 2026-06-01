using System.Data;
using Dapper;
using Data.Sql;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using OpenPlot.Features.Export;
using OpenPlot.Api.Services.Security;
using OpenPlot.Data.Dtos;

public static class ExportEndpoints
{
    private const string IncompleteRunErrorMessage = "A consulta n\u00E3o est\u00E1 conclu\u00EDda. S\u00F3 \u00E9 poss\u00EDvel converter consultas completas/\u00EDntegras.";
    private const string InvalidRunIdMessage = "run_id inv\u00E1lido";
    private const string RequiredFormatMessage = "format \u00E9 obrigat\u00F3rio";
    private const string UnsupportedFormatMessage = "Formato de exporta\u00E7\u00E3o ainda n\u00E3o suportado";
    private const string RunNotFoundMessage = "run n\u00E3o encontrada.";
    private const string ExportNotCompletedMessage = "export ainda n\u00E3o conclu\u00EDdo";
    private const string ExportFileNotLocatedMessage = "arquivo de exporta\u00E7\u00E3o n\u00E3o localizado.";
    private const string ExportFileNotFoundOnDiskMessage = "arquivo de exporta\u00E7\u00E3o n\u00E3o encontrado em disco.";

    internal static bool CanConvertSearchRun(string? runStatus) =>
        string.Equals(runStatus, "completed", StringComparison.OrdinalIgnoreCase)
        || string.Equals(runStatus, "done", StringComparison.OrdinalIgnoreCase);

    internal static object BuildIncompleteRunError(string? runStatus) => new
    {
        error = IncompleteRunErrorMessage,
        status = runStatus
    };

    public static IEndpointRouteBuilder MapExport(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/export")
            .WithTags("Export")
            .RequireAuthorization();

        static bool IsSupportedFormat(string? format) =>
            string.Equals(format?.Trim(), "comtrade", StringComparison.OrdinalIgnoreCase);

        group.MapPost("", async (
            HttpContext http,
            [FromServices] IUserContextAccessor userContextAccessor,
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] IExportFileService exportFileService,
            [FromBody] QueueExportRequest req,
            CancellationToken ct
        ) =>
        {
            var username = userContextAccessor.GetUsername(http);

            if (string.IsNullOrWhiteSpace(username))
                return Results.Unauthorized();

            var runIdRaw = req.ResolveRunId()?.Trim();
            if (!Guid.TryParse(runIdRaw, out var runId) || runId == Guid.Empty)
                return Results.BadRequest("run_id inválido");

            var format = req.format?.Trim();
            if (string.IsNullOrWhiteSpace(format))
                return Results.BadRequest("format é obrigatório");

            if (!IsSupportedFormat(format))
                return Results.BadRequest(new { error = "Formato de exportação ainda não suportado", format });

            using var db = dbf.Create();
            await exportFileService.PurgeExpiredExportsAsync(db, ct);

            var runStatus = await db.QuerySingleOrDefaultAsync<string?>(@"
SELECT status
FROM openplot.search_runs
WHERE id = @run_id
LIMIT 1;", new { run_id = runId });

            if (runStatus is null)
                return Results.NotFound("run não encontrada.");

            if (!CanConvertSearchRun(runStatus))
                return Results.BadRequest(BuildIncompleteRunError(runStatus));

            await db.ExecuteAsync(ExportSql.QueueExportRun, new { run_id = runId });

            var row = await db.QuerySingleOrDefaultAsync<ExportRunStatusRow>(
                ExportSql.GetExportRunStatus,
                new { run_id = runId, username }
            );

            return Results.Accepted($"/export/{format.ToLowerInvariant()}/{runId}", new
            {
                runId,
                format = row?.format ?? format.ToLowerInvariant(),
                status = row?.status ?? "queued",
                progress = row?.progress ?? 0,
                message = row?.message ?? "Na fila"
            });
        });

        group.MapGet("/{format}/{id:guid}", async (
            string format,
            Guid id,
            HttpContext http,
            [FromServices] IUserContextAccessor userContextAccessor,
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] IExportFileService exportFileService,
            CancellationToken ct
        ) =>
        {
            var username = userContextAccessor.GetUsername(http);

            if (string.IsNullOrWhiteSpace(username))
                return Results.Unauthorized();

            if (!IsSupportedFormat(format))
                return Results.BadRequest(new { error = "Formato de exportação ainda não suportado", format });

            using var db = dbf.Create();
            await exportFileService.PurgeExpiredExportsAsync(db, ct);

            var row = await db.QuerySingleOrDefaultAsync<ExportRunStatusRow>(
                ExportSql.GetExportRunStatus,
                new { run_id = id, username }
            );

            if (row is null)
                return Results.NotFound();

            if (!string.Equals(row.format, format, StringComparison.OrdinalIgnoreCase))
                return Results.NotFound();

            return Results.Ok(row);
        });

        group.MapGet("/{format}/{id:guid}/file", async (
            string format,
            Guid id,
            HttpContext http,
            [FromServices] IUserContextAccessor userContextAccessor,
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] IExportFileService exportFileService,
            CancellationToken ct
        ) =>
        {
            var username = userContextAccessor.GetUsername(http);

            if (string.IsNullOrWhiteSpace(username))
                return Results.Unauthorized();

            if (!IsSupportedFormat(format))
                return Results.BadRequest(new { error = "Formato de exportação ainda não suportado", format });

            using var db = dbf.Create();
            await exportFileService.PurgeExpiredExportsAsync(db, ct);

            var row = await db.QuerySingleOrDefaultAsync<ExportRunStatusRow>(
                ExportSql.GetExportRunStatus,
                new { run_id = id, username }
            );

            if (row is null)
                return Results.NotFound();

            if (!string.Equals(row.format, format, StringComparison.OrdinalIgnoreCase))
                return Results.NotFound();

            if (!string.Equals(row.status, "done", StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest(new { error = "export ainda não concluído", status = row.status, progress = row.progress });

            return exportFileService.ResolveFileResult(row);
        });

        return app;
    }
}
