namespace OpenPlot.Api.Services.Logging;

public sealed class ApiRequestLogEntry
{
    public DateTime TimestampUtc { get; init; }
    public string Method { get; init; } = default!;
    public string Path { get; init; } = default!;
    public int StatusCode { get; init; }
    public int ElapsedMs { get; init; }
    public string? UserName { get; init; }
    public string? UserId { get; init; }
    public string? Ip { get; init; }
    public string? CorrelationId { get; init; }
    public string? UserAgent { get; init; }

    public string? Protocol { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }

    public string? RequestBody { get; init; }

    public string? QueryString { get; init; }
}

