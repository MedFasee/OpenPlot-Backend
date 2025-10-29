namespace OpenPlot.Auth.Contracts.Responses;

public sealed class ApiResponse<T>
{
    public required int Status { get; init; }
    public required T Data { get; init; }
}
