using System.Threading;
using System.Threading.Tasks;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Contracts.Responses;

namespace OpenPlot.Auth.Services
{
    public sealed class RealAuthService : IAuthService
    {
        // Dependências futuras (Keycloak, HttpClient, JWT handler, etc.)
        // private readonly IHttpClientFactory _http;
        // private readonly IConfiguration _cfg;

        public RealAuthService()
        {
            // _http = http;
            // _cfg = cfg;
        }

        public async Task<(bool ok, LoginResponse? resp, string? error)> AuthenticateAsync(
            LoginRequest request, CancellationToken ct)
        {
            // ================================================
            // Fase 1 — MOCK PROD (placeholder)
            // ================================================
            // Aqui retornamos erro até o Keycloak ser integrado
            await Task.CompletedTask;
            return (false, null, "Auth real (Keycloak) ainda não implementada.");

            // ================================================
            // Fase 2 — Exemplo de fluxo OIDC real (futuro)
            // ================================================
            /*
            var tokenEndpoint = _cfg["Auth:Keycloak:TokenEndpoint"];
            var clientId = _cfg["Auth:Keycloak:ClientId"];
            var clientSecret = _cfg["Auth:Keycloak:ClientSecret"];

            var http = _http.CreateClient();
            var form = new Dictionary<string, string>
            {
                ["grant_type"] = "password",
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["username"] = request.Username,
                ["password"] = request.Password
            };

            var resp = await http.PostAsync(tokenEndpoint, new FormUrlEncodedContent(form), ct);
            if (!resp.IsSuccessStatusCode)
                return (false, null, "Falha na autenticação Keycloak");

            var json = await resp.Content.ReadAsStringAsync(ct);
            var token = JsonDocument.Parse(json).RootElement;

            var jwt = token.GetProperty("access_token").GetString();

            // (Aqui você poderia decodificar o JWT e montar LoginResponse)
            var info = DecodeJwt(jwt!);
            var loginResp = new LoginResponse
            {
                Sub = info.Sub,
                Username = info.PreferredUsername,
                Roles = info.Roles,
                Email = info.Email,
                Claims = info.Claims,
                SessionId = Guid.NewGuid().ToString("N")
            };
            return (true, loginResp, null);
            */
        }

        // private JwtInfo DecodeJwt(string token) { ... }
    }
}
