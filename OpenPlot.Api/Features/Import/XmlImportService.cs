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
        /*var connectionString = _configuration.GetConnectionString("Db")
            ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres";*/

        var connectionString = _configuration.GetConnectionString("Db")
            ?? "Host=ydrmkeftlc.upkcvavk1z.privatelink.cloud.tigerdata.com;Port=5432;Database=tsdb;Username=servicedopenplot;Password=Zxolgm370$";

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
