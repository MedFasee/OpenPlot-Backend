using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;

namespace OpenPlot.Features.Import;

public static class ImportEndpoints
{
    public static IEndpointRouteBuilder MapImport(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("")
            .WithTags("Import_XML")
            .RequireAuthorization();

        group.MapPost("/xml/import", async (
            ImportXmlRequest req,
            IXmlImportService importService,
            IConfiguration configuration,
            CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "Path obrigatório." });

            var configuredRoot = configuration["XmlFolder"];
            if (string.IsNullOrWhiteSpace(configuredRoot))
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Importação XML não configurada",
                    detail: "Defina XmlFolder para habilitar a importação.");
            }

            string rootPath;
            string requestedPath;

            try
            {
                rootPath = Path.GetFullPath(configuredRoot);
                requestedPath = Path.IsPathRooted(req.Path)
                    ? Path.GetFullPath(req.Path)
                    : Path.GetFullPath(Path.Combine(rootPath, req.Path));
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
            {
                return Results.BadRequest(new { error = "Path inválido." });
            }

            if (!IsPathInsideRoot(rootPath, requestedPath))
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status403Forbidden,
                    title: "Path não permitido",
                    detail: "A importação está restrita ao diretório configurado em XmlFolder.");
            }

            if (!Directory.Exists(requestedPath))
                return Results.BadRequest(new { error = "Diretório não encontrado." });

            var summaries = await importService.ImportAsync(requestedPath, ct);
            return Results.Ok(new { status = 200, data = summaries });
        })
        .WithTags("Import")
        .Produces(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status401Unauthorized)
        .Produces(StatusCodes.Status403Forbidden)
        .Produces(StatusCodes.Status503ServiceUnavailable);

        return app;
    }

    private static bool IsPathInsideRoot(string rootPath, string requestedPath)
    {
        var relative = Path.GetRelativePath(rootPath, requestedPath);
        if (relative == ".")
            return true;

        return !Path.IsPathRooted(relative)
            && !relative.Equals("..", StringComparison.Ordinal)
            && !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            && !relative.StartsWith(".." + Path.AltDirectorySeparatorChar, StringComparison.Ordinal);
    }

    public sealed class ImportXmlRequest
    {
        [Required]
        public string Path { get; set; } = string.Empty;
    }
}
