namespace OpenPlot.Auth.Contracts.Responses;

public sealed class LoginEnvelope
{
    public required string Token { get; init; }
    public required UsuarioDto Usuario { get; init; }
}

public sealed class UsuarioDto
{
    public required string Nome { get; init; }
    public required string Email { get; init; }
    public required string NomePref { get; init; }
    public required string Role { get; init; } // "admin" | "editor" | "reader"
}
