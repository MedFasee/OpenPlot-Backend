using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Infrastructure.Auth;

namespace OpenPlot.Features.Sso.Services;

public interface ISsoIdentityService
{
    Task<(bool Ok, LoginResponse? Response, string? Error)> ResolveAsync(string clientId, string consultaId, CancellationToken ct = default);
}

public sealed class SsoIdentityService : ISsoIdentityService
{
    private readonly ISsoClientRegistry _clientRegistry;
    private readonly IUserStore _userStore;

    public SsoIdentityService(ISsoClientRegistry clientRegistry, IUserStore userStore)
    {
        _clientRegistry = clientRegistry;
        _userStore = userStore;
    }

    public async Task<(bool Ok, LoginResponse? Response, string? Error)> ResolveAsync(string clientId, string consultaId, CancellationToken ct = default)
    {
        if (!_clientRegistry.TryGetClient(clientId, out var client))
            return (false, null, "Cliente SSO não cadastrado.");

        if (string.IsNullOrWhiteSpace(client.TechnicalUsername))
            return (false, null, "Cliente SSO sem usuário técnico configurado.");

        var user = await _userStore.FindByUsernameAsync(client.TechnicalUsername, ct);
        if (user is null)
            return (false, null, $"Usuário técnico '{client.TechnicalUsername}' não encontrado.");

        var claims = new Dictionary<string, string>(user.claims, StringComparer.OrdinalIgnoreCase)
        {
            ["auth_flow"] = "sso",
            ["origin_client"] = client.ClientId,
            ["consulta_id"] = consultaId
        };

        var response = new LoginResponse
        {
            Sub = user.sub,
            Username = user.username,
            PreferredUsername = user.preferred_username,
            Roles = user.roles,
            Email = user.email,
            Claims = claims,
            SessionId = Guid.NewGuid().ToString("N")
        };

        return (true, response, null);
    }
}
