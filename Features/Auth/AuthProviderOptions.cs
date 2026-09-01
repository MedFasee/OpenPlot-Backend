namespace OpenPlot.Features.Auth;

public sealed class AuthProviderOptions
{
    public string Provider { get; set; } = "OpenPlot";

    public bool IsOpenPlot =>
        string.Equals(Provider?.Trim(), "OpenPlot", StringComparison.OrdinalIgnoreCase);

    public bool IsOns =>
        string.Equals(Provider?.Trim(), "Ons", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Provider?.Trim(), "OnsSso", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Provider?.Trim(), "Sso", StringComparison.OrdinalIgnoreCase);

    public bool IsSupported => IsOpenPlot || IsOns;

    public string Current => IsOns ? "Ons" : IsOpenPlot ? "OpenPlot" : (Provider?.Trim() ?? string.Empty);
}
