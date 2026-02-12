using Microsoft.AspNetCore.Mvc;

namespace OpenPlot.Features.Runs.Contracts;

public sealed record WindowQuery(DateTime? From, DateTime? To)
{
    public DateTime? FromUtc => From?.ToUniversalTime();
    public DateTime? ToUtc => To?.ToUniversalTime();
}

public sealed record SimpleSeriesQuery(
    [property: FromQuery(Name = "run_Id")] Guid RunId,
    int MaxPoints = 5000,
    string? Unit = "raw"
);

public sealed record SeqRunQuery(
    [property: FromQuery(Name = "run_Id")] Guid RunId,
    [property: FromQuery] int MaxPoints = 5000,
    [property: FromQuery] string? Unit = "raw",     // raw|pu
    [property: FromQuery] double? VoltLevel = null, // usado se unit=pu e kind=voltage
    [property: FromQuery] string? Seq = null,       // pos|neg|zero
    [property: FromQuery] string? Kind = null       // voltage|current
);

public sealed record UnbalanceRunQuery(
    [property: FromQuery(Name = "run_Id")] Guid RunId,
    [property: FromQuery] int MaxPoints = 5000,
    [property: FromQuery] double? VoltLevel = null,
    [property: FromQuery] string? Kind = null // voltage|current
);
