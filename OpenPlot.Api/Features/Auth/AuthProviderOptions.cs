namespace OpenPlot.Features.Auth;

public sealed class AuthProviderOptions
{
    public string Provider { get; set; } = "OpenPlot";

    public bool IsOpenPlot =>
        string.Equals(Provider, "OpenPlot", StringComparison.OrdinalIgnoreCase);

    public bool IsOns =>
        string.Equals(Provider, "Ons", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Provider, "OnsSso", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Provider, "Sso", StringComparison.OrdinalIgnoreCase);

    public string Current => IsOns ? "Ons" : "OpenPlot";
}
