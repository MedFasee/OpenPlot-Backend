using Npgsql;

namespace OpenPlot.Features.Import;

internal sealed class XmlCatalogImporter
{
    private readonly string _connectionString;
    private readonly IXmlCatalogParser _parser;
    private readonly IXmlCatalogPersistence _persistence;

    internal XmlCatalogImporter(
        string connectionString,
        IXmlCatalogParser? parser = null,
        IXmlCatalogPersistence? persistence = null)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _parser = parser ?? new XmlCatalogParser();
        _persistence = persistence ?? new XmlCatalogPersistence();
    }

    internal sealed class ImportSummary
    {
        public string File { get; set; } = "";
        public int PdcId { get; set; }
        public int Pmus { get; set; }
        public int Signals { get; set; }
        public List<string> Notes { get; } = new();
    }

    internal async Task<List<ImportSummary>> RunAsync(string xmlPathOrFolder, CancellationToken ct = default)
    {
        var summaries = new List<ImportSummary>();
        var files = ResolveFiles(xmlPathOrFolder);
        if (files.Length == 0)
            return summaries;

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        foreach (var path in files)
        {
            try
            {
                var parsedFile = _parser.Parse(path);
                var summary = await _persistence.PersistAsync(parsedFile, conn, ct);
                summaries.Add(summary);
            }
            catch (Exception ex)
            {
                var summary = new ImportSummary { File = path };
                summary.Notes.Add("Erro: " + ex.Message);
                summaries.Add(summary);
            }
        }

        return summaries;
    }

    private static string[] ResolveFiles(string pathOrFolder)
    {
        if (File.Exists(pathOrFolder))
            return new[] { pathOrFolder };

        if (Directory.Exists(pathOrFolder))
            return Directory.GetFiles(pathOrFolder, "*.xml", SearchOption.TopDirectoryOnly);

        return Array.Empty<string>();
    }
}
