using Dapper;
using Data.Sql;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using OpenPlot.Data.Dtos;

public static class ExportEndpoints
{
    public static IEndpointRouteBuilder MapExport(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/export")
            .WithTags("Export")
            .RequireAuthorization();

        static bool IsSupportedFormat(string? format) =>
            string.Equals(format?.Trim(), "comtrade", StringComparison.OrdinalIgnoreCase);

        group.MapPost("", async (
            HttpContext http,
            [FromServices] IDbConnectionFactory dbf,
            [FromBody] QueueExportRequest req
        ) =>
        {
            var username =
                http.User?.FindFirst("username")?.Value
                ?? http.User?.FindFirst("unique_name")?.Value
                ?? http.User?.Identity?.Name;

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

            var runStatus = await db.QuerySingleOrDefaultAsync<string?>(@"
SELECT status
FROM openplot.search_runs
WHERE id = @run_id
LIMIT 1;", new { run_id = runId });

            if (runStatus is null)
                return Results.NotFound("run não encontrada.");

            if (!string.Equals(runStatus, "done", StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest(new
                {
                    error = "A consulta não está concluída. Só é possível converter consultas completas/íntegras.",
                    status = runStatus
                });

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
            [FromServices] IDbConnectionFactory dbf
        ) =>
        {
            var username =
                http.User?.FindFirst("username")?.Value
                ?? http.User?.FindFirst("unique_name")?.Value
                ?? http.User?.Identity?.Name;

            if (string.IsNullOrWhiteSpace(username))
                return Results.Unauthorized();

            if (!IsSupportedFormat(format))
                return Results.BadRequest(new { error = "Formato de exportação ainda não suportado", format });

            using var db = dbf.Create();

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
            [FromServices] IDbConnectionFactory dbf
        ) =>
        {
            var username =
                http.User?.FindFirst("username")?.Value
                ?? http.User?.FindFirst("unique_name")?.Value
                ?? http.User?.Identity?.Name;

            if (string.IsNullOrWhiteSpace(username))
                return Results.Unauthorized();

            if (!IsSupportedFormat(format))
                return Results.BadRequest(new { error = "Formato de exportação ainda não suportado", format });

            using var db = dbf.Create();

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

            if (string.IsNullOrWhiteSpace(row.dir_path) || string.IsNullOrWhiteSpace(row.file_name))
                return Results.NotFound("arquivo de exportação não localizado.");

            var fullPath = Path.Combine(row.dir_path, row.file_name);
            if (!File.Exists(fullPath))
                return Results.NotFound("arquivo de exportação não encontrado em disco.");

            var contentType = string.Equals(Path.GetExtension(row.file_name), ".zip", StringComparison.OrdinalIgnoreCase)
                ? "application/zip"
                : "application/octet-stream";

            return Results.File(fullPath, contentType, row.file_name);
        });

        return app;
    }
}
