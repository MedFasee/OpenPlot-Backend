namespace OpenPlot.Auth.Infrastructure.Auth.Options;

public sealed class AuthOptions
{
    public bool UseMock { get; set; } = true;     // dev: true; prod: false
    public string? Issuer { get; set; }
    public string? Audience { get; set; }
    public string? SigningKey { get; set; }
}
