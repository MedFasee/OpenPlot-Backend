using System.Text.Json.Serialization;

namespace OpenPlot.Auth.Contracts.Responses;


public sealed class LoginResponse
{
    // Identidade
    public required string Sub { get; init; }
    public required string Username { get; init; }
    [JsonPropertyName("preferred_username")]
    public string? PreferredUsername { get; set; }
    // Autorização
    public required IReadOnlyList<string> Roles { get; init; }

    // Sessão
    public required string SessionId { get; init; }

    // Contato
    public string? Email { get; init; }

    // JWT / extras
    public IDictionary<string, string>? Claims { get; init; }
    public string? Token { get; init; }
}
