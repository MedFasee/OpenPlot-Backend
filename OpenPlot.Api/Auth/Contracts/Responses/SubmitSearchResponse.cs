namespace OpenPlot.Auth.Contracts.Responses;

public sealed class SubmitSearchResponse
{
    public required string RequestId { get; init; }
    public required string Username { get; init; }
    public required string ConfigVersion { get; init; }
    public required IReadOnlyList<string> Terminais { get; init; }

    // Resolução normalizada
    public required string Mode { get; init; }   // "agg" | "rate"
    public string? Agg { get; init; }
    public int? SelectRate { get; init; }
}
