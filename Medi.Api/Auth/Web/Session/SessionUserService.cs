using System.Text.Json;
using OpenPlot.Auth.Contracts.Responses;

namespace OpenPlot.Auth.Web.Session;

public sealed class SessionUserService : ISessionUserService
{
    private readonly IHttpContextAccessor _http;
    private readonly string _key;

    public SessionUserService(IHttpContextAccessor http, IConfiguration cfg)
    {
        _http = http;
        _key = cfg["Auth:SessionKey"] ?? "openplot:user";
    }

    public void SetCurrentUser(LoginResponse user)
    {
        var json = JsonSerializer.Serialize(user);
        _http.HttpContext!.Session.SetString(_key, json);
    }

    public LoginResponse? GetCurrentUser()
    {
        var json = _http.HttpContext!.Session.GetString(_key);
        return string.IsNullOrWhiteSpace(json) ? null : JsonSerializer.Deserialize<LoginResponse>(json);
    }

    public void Clear() => _http.HttpContext!.Session.Remove(_key);
}
