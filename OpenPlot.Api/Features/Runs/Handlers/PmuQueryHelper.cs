using Dapper;

namespace OpenPlot.Features.Runs.Handlers;

public interface IPmuQueryHelper
{
    string[] Normalize(params IEnumerable<string?>?[] sources);
    string[] NormalizeExcluding(string excluded, params IEnumerable<string?>?[] sources);
    string BuildOrSqlFilter(string sqlField, IReadOnlyList<string> pmuNames, string parameterPrefix = "pmu");
    void AddSqlParameters(DynamicParameters parameters, IReadOnlyList<string> pmuNames, string parameterPrefix = "pmu");
}

public sealed class PmuQueryHelper : IPmuQueryHelper
{
    public string[] Normalize(params IEnumerable<string?>?[] sources)
    {
        return sources
            .Where(s => s is not null)
            .SelectMany(s => s!)
            .Select(s => s?.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray()!;
    }

    public string[] NormalizeExcluding(string excluded, params IEnumerable<string?>?[] sources)
    {
        var normalized = Normalize(sources);
        if (string.IsNullOrWhiteSpace(excluded)) return normalized;

        return normalized
            .Where(p => !p.Equals(excluded.Trim(), StringComparison.OrdinalIgnoreCase))
            .ToArray();
    }

    public string BuildOrSqlFilter(string sqlField, IReadOnlyList<string> pmuNames, string parameterPrefix = "pmu")
    {
        if (pmuNames.Count == 0) return "TRUE";

        return string.Join(" OR ", pmuNames.Select((_, i) => $"LOWER({sqlField}) = LOWER(@{parameterPrefix}{i})"));
    }

    public void AddSqlParameters(DynamicParameters parameters, IReadOnlyList<string> pmuNames, string parameterPrefix = "pmu")
    {
        for (int i = 0; i < pmuNames.Count; i++)
            parameters.Add($"{parameterPrefix}{i}", pmuNames[i]);
    }
}
