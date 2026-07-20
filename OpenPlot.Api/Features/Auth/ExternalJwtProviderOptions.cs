namespace OpenPlot.Features.Auth;

public sealed class ExternalJwtProviderOptions
{
    public string AuthenticationScheme { get; set; } = string.Empty;
    public bool Enabled { get; set; } = true;
    public string? Authority { get; set; }
    public string? MetadataAddress { get; set; }
    public bool RequireHttpsMetadata { get; set; } = true;
    public bool SaveToken { get; set; } = true;
    public bool IncludeErrorDetails { get; set; } = true;
    public string? Audience { get; set; }
    public List<string> ValidAudiences { get; set; } = [];
    public string? ValidIssuer { get; set; }
    public List<string> ValidIssuers { get; set; } = [];
    public string? NameClaimType { get; set; }
    public string? RoleClaimType { get; set; }
    public int ClockSkewMinutes { get; set; } = 2;

    public IEnumerable<string> ResolveIssuerCandidates()
    {
        if (!string.IsNullOrWhiteSpace(ValidIssuer))
            yield return ValidIssuer;

        foreach (var issuer in ValidIssuers.Where(x => !string.IsNullOrWhiteSpace(x)))
            yield return issuer;

        if (!string.IsNullOrWhiteSpace(Authority))
            yield return Authority;
    }
}
