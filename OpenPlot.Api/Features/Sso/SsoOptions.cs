namespace OpenPlot.Features.Sso;

public sealed class SsoOptions
{
    public string? FrontendBaseUrl { get; set; }
    public string ConsumePath { get; set; } = "/sso/consumir";
    public string RedirectPathTemplate { get; set; } = "/processar-busca?consultaId={consultaId}";
    public int RequestTtlSeconds { get; set; } = 300;
    public int LoginTokenTtlSeconds { get; set; } = 120;
}

public sealed class SsoClientOptions
{
    public string ClientId { get; set; } = string.Empty;
    public string Secret { get; set; } = string.Empty;
    public string TechnicalUsername { get; set; } = string.Empty;
}
