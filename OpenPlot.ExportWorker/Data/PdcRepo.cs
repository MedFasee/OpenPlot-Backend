using Dapper;

namespace OpenPlot.ExportWorker.Data;

public sealed class PdcRepo
{
    private readonly Db _db;
    public PdcRepo(Db db) => _db = db;

    public async Task<int?> GetFpsByNameAsync(string pdcName, CancellationToken ct)
    {
        const string sql = @"
SELECT fps
FROM openplot.pdc
WHERE LOWER(name) = LOWER(@name)
LIMIT 1;
";
        return await _db.Conn.QueryFirstOrDefaultAsync<int?>(
            new CommandDefinition(sql, new { name = pdcName }, cancellationToken: ct));
    }
}