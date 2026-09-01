namespace OpenPlot.Features.Import;

public sealed class ImportSummaryDto
{
    public string File { get; init; } = "";
    public int PdcId { get; init; }
    public int Pmus { get; init; }
    public int Signals { get; init; }
    public IReadOnlyList<string> Notes { get; init; } = Array.Empty<string>();
}

public interface IXmlImportService
{
    Task<IReadOnlyList<ImportSummaryDto>> ImportAsync(string path, CancellationToken ct);
}

public sealed class XmlImportService : IXmlImportService
{
    private readonly IConfiguration _configuration;

    public XmlImportService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task<IReadOnlyList<ImportSummaryDto>> ImportAsync(string path, CancellationToken ct)
    {
        var connectionString = _configuration.GetConnectionString("Db");
        if (string.IsNullOrWhiteSpace(connectionString))
            connectionString = Environment.GetEnvironmentVariable("OPENPLOT_DB_CONNECTION");

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException(
                "Defina ConnectionStrings:Db ou OPENPLOT_DB_CONNECTION para o banco externo.");

        var importer = new XmlCatalogImporter(connectionString);
        var summaries = await importer.RunAsync(path, ct);

        return summaries
            .Select(summary => new ImportSummaryDto
            {
                File = summary.File,
                PdcId = summary.PdcId,
                Pmus = summary.Pmus,
                Signals = summary.Signals,
                Notes = summary.Notes.ToArray()
            })
            .ToList();
    }
}
