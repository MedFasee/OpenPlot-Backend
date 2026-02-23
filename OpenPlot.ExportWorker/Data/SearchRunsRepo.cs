using Dapper;
using OpenPlot.ExportWorker.Domain;
using System.Text.Json;

namespace OpenPlot.ExportWorker.Data;

public sealed class SearchRunsRepo
{
    private readonly Db _db;
    public SearchRunsRepo(Db db) => _db = db;

    private sealed class Row
    {
        public Guid Id { get; set; }
        public string Source { get; set; } = "";
        public string Label { get; set; } = "";
        public DateTimeOffset From_Ts { get; set; }
        public DateTimeOffset To_Ts { get; set; }
        public int Select_Rate { get; set; }
        public string? Pmus_Ok { get; set; }
        public string? Pmus { get; set; }
    }

    public async Task<RunContext?> LoadRunContextAsync(Guid runId, CancellationToken ct)
    {
        const string sql = @"
SELECT
  id,
  source,
  COALESCE(label,'') AS label,
  from_ts AS from_ts,
  to_ts   AS to_ts,
  select_rate,
  pmus_ok,
  pmus
FROM openplot.search_runs
WHERE id = @run_id::uuid;
";

        var row = await _db.Conn.QueryFirstOrDefaultAsync<Row>(
            new CommandDefinition(sql, new { run_id = runId }, cancellationToken: ct));

        if (row is null) return null;

        var pmus = ParseJsonStringArray(row.Pmus_Ok) ?? ParseJsonStringArray(row.Pmus) ?? Array.Empty<string>();

        return new RunContext(
            RunId: row.Id,
            PdcName: row.Source,
            Label: row.Label,
            FromUtc: row.From_Ts.ToUniversalTime(),
            ToUtc: row.To_Ts.ToUniversalTime(),
            SelectRate: row.Select_Rate,
            RunPmus: pmus,
            PmuFilter: null
        );
    }

    private static string[]? ParseJsonStringArray(string? json)
    {
        if (string.IsNullOrWhiteSpace(json)) return null;
        try { return JsonSerializer.Deserialize<string[]>(json); }
        catch { return null; }
    }
}