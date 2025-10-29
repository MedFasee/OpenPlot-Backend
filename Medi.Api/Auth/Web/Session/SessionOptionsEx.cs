namespace OpenPlot.Auth.Web.Session;

public sealed class SessionOptionsEx
{
    public string CookieName { get; set; } = "openplot_session";
    public int IdleTimeoutMinutes { get; set; } = 60;
    public bool UseMock { get; set; } = true;
}
