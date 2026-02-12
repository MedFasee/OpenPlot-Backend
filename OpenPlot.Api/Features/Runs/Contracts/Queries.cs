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
