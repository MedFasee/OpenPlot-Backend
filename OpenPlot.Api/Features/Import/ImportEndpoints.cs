using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http;
using System.ComponentModel.DataAnnotations;

namespace OpenPlot.Features.Import;

public static class ImportEndpoints
{
    public static IEndpointRouteBuilder MapImport(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("")
                     .WithTags("Import_XML");


        // POST /import/xml/import  → body: { "path": "C:\\pasta\\com\\xmls" }
        group.MapPost("/xml/import", async (
            ImportXmlRequest req,
            IXmlImportService importService,
            CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "Path obrigatório." });

            var summaries = await importService.ImportAsync(req.Path, ct);
            return Results.Json(new { status = 200, data = summaries });
        })
        .WithTags("Import"); // ajuda a organizar no Swagger

        return app;
    }

    public sealed class ImportXmlRequest
    {
        [Required]
        public string Path { get; set; } = "";
    }
}
