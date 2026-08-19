using Npgsql;
using NpgsqlTypes;

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
        await EnsureCatalogConflictTargetsAsync(conn, ct);

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

    private static async Task EnsureCatalogConflictTargetsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        var requiredTargets = new[]
        {
            (Table: "pdc", Columns: new[] { "name" }),
            (Table: "pmu", Columns: new[] { "id_name" }),
            (Table: "pdc_pmu", Columns: new[] { "pdc_id", "pmu_id" }),
            (Table: "signal", Columns: new[] { "pdc_pmu_id", "name", "phase", "component" })
        };

        const string sql = @"
SELECT EXISTS (
    SELECT 1
    FROM pg_index i
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'openplot'
      AND t.relname = @table_name
      AND i.indisunique
      AND i.indpred IS NULL
      AND (
          SELECT array_agg(a.attname::text ORDER BY ord.n)
          FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, n)
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
      ) = @columns::text[]
);";

        var missingTargets = new List<string>();

        foreach (var target in requiredTargets)
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("table_name", target.Table);
            cmd.Parameters.Add("columns", NpgsqlDbType.Array | NpgsqlDbType.Text).Value = target.Columns;

            var exists = Convert.ToBoolean(await cmd.ExecuteScalarAsync(ct));
            if (!exists)
                missingTargets.Add($"openplot.{target.Table} ({string.Join(", ", target.Columns)})");
        }

        if (missingTargets.Count == 0)
            return;

        throw new InvalidOperationException(
            "Schema incompatível para import XML. Faltam índices/constraints únicos para ON CONFLICT em: "
            + string.Join("; ", missingTargets)
            + ". Execute o script scripts/06_fix_catalog_unique_indexes.sql.");
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
