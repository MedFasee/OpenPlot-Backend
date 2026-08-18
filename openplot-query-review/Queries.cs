using Microsoft.AspNetCore.Mvc;
using OpenPlot.Features.Runs.Handlers.Abstractions;

namespace OpenPlot.Features.Runs.Contracts;

public sealed record WindowQuery(DateTime? From, DateTime? To)
{
    public DateTime? FromUtc => From?.ToUniversalTime();
    public DateTime? ToUtc => To?.ToUniversalTime();
}

public sealed class SimpleSeriesQuery : ISeriesQuery
{
    [FromQuery(Name = "run_Id")]
    public Guid RunId { get; init; }

    [FromQuery(Name = "maxPoints")]
    public string? MaxPoints { get; init; }

    public bool MaxPointsIsAll =>
        string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

    public int ResolveMaxPoints(int @default = 5000)
    {
        if (MaxPointsIsAll) return int.MaxValue;
        if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
        return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
    }
}

public sealed record SeqRunQuery(
    [property: FromQuery(Name = "run_Id")] Guid RunId,
    [property: FromQuery(Name = "maxPoints")] string? MaxPoints,
    [property: FromQuery] string? Unit = "raw",      // raw|pu
    [property: FromQuery] double? VoltLevel = null,  // usado se unit=pu e kind=voltage
    [property: FromQuery] string? Seq = null,        // pos|neg|zero (se ainda usar)
    [property: FromQuery] string? Kind = null        // voltage|current (se ainda usar)
)
{
    public bool MaxPointsIsAll =>
        string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

    public int ResolveMaxPoints(int @default = 5000)
    {
        if (MaxPointsIsAll) return int.MaxValue;
        if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
        return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
    }
}

public sealed record UnbalanceRunQuery(
    [property: FromQuery(Name = "run_Id")] Guid RunId,
    [property: FromQuery(Name = "maxPoints")] string? MaxPoints,
    [property: FromQuery] double? VoltLevel = null,
    [property: FromQuery] string? Kind = null        // voltage|current
)
{
    public bool MaxPointsIsAll =>
        string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

    public int ResolveMaxPoints(int @default = 5000)
    {
        if (MaxPointsIsAll) return int.MaxValue;
        if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
        return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
    }
}
