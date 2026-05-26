using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.XmlImporter;

internal static class Program
{
    private const string DefaultConnectionString = "Host=postgres;Port=5432;Database=postgres;Username=postgres;Password=postgres";
    private const string DefaultImportPath = "/data/xml";

    private static async Task<int> Main(string[] args)
    {
        var connectionString = ReadSetting("OPENPLOT_XMLIMPORTER__CONNECTION_STRING", "OPENPLOT_XMLIMPORTER__DB")
            ?? DefaultConnectionString;

        var importPath = args.FirstOrDefault()
            ?? ReadSetting("OPENPLOT_XMLIMPORTER__PATH", "XML_IMPORT_PATH")
            ?? DefaultImportPath;

        if (!File.Exists(importPath) && !Directory.Exists(importPath))
        {
            Console.Error.WriteLine($"Caminho de importação não encontrado: {importPath}");
            return 1;
        }

        var importer = new XmlImporter(connectionString);
        var summaries = await importer.RunAsync(importPath, CancellationToken.None);

        Console.WriteLine($"Importação concluída. Arquivos processados: {summaries.Count}");

        foreach (var summary in summaries)
        {
            Console.WriteLine($"- {summary.File} | PdcId={summary.PdcId} | PMUs={summary.Pmus} | Signals={summary.Signals}");

            foreach (var note in summary.Notes)
                Console.WriteLine($"  nota: {note}");
        }

        return 0;
    }

    private static string? ReadSetting(params string[] keys)
    {
        foreach (var key in keys)
        {
            var value = Environment.GetEnvironmentVariable(key);
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }
}
