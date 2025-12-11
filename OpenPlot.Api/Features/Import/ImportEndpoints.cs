using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http;
using System.ComponentModel.DataAnnotations;
using OpenPlot.XmlImporter;

namespace OpenPlot.Features.Import;

public static class ImportEndpoints
{
    public static IEndpointRouteBuilder MapImport(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("")
                     .WithTags("Import_XML");


        // POST /import/xml  → body: { "path": "C:\\pasta\\com\\xmls" }
        group.MapPost("/xml", async (
            ImportXmlRequest req,
            IConfiguration cfg,
            CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "Path obrigatório." });

            var cs = cfg.GetConnectionString("Db")
                   // ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres";
                   ?? "Host=localhost;Port=5432;Database=openplot;Username=postgres;Password=postgres";

            var importer = new OpenPlot.XmlImporter.XmlImporter(cs);
            var summaries = await importer.RunAsync(req.Path, ct);
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
