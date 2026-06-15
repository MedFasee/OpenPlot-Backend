namespace OpenPlot.Features.Sso.Contracts;

public sealed class CreateSsoLinkResponse
{
    public required string ClientId { get; init; }
    public required string ConsultaId { get; init; }
    public string Token { get; set; } = string.Empty;
    public required string Url { get; init; }
    public required DateTime ExpiresAtUtc { get; init; }
}
