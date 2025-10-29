namespace OpenPlot.Auth.Contracts.Responses;

public sealed class LoginResponse
{
    public required string Sub { get; init; }
    public required string Username { get; init; }
    public required IReadOnlyList<string> Roles { get; init; }
    public required string SessionId { get; init; }
    public string? Email { get; init; }
    public IDictionary<string, string>? Claims { get; init; }
    public string? Token { get; init; }
    // (DisplayName é opcional; se não existir, usamos Username no controller)
    public string? DisplayName { get; init; }

}
