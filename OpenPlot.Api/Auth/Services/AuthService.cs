using System;
using System.Threading;
using System.Threading.Tasks;
using OpenPlot.Auth.Infrastructure.Auth;        // IUserStore
using OpenPlot.Auth.Contracts.Requests;         // LoginRequest
using OpenPlot.Auth.Contracts.Responses;        // LoginResponse
using OpenPlot.Auth.Web.Session;                // ISessionUserService

namespace OpenPlot.Auth.Services
{
    public sealed class AuthService : IAuthService
    {
        private readonly IUserStore _store;
        private readonly ISessionUserService _sessionUser;

        public AuthService(IUserStore store, ISessionUserService sessionUser)
        {
            _store = store;
            _sessionUser = sessionUser;
        }

        public async Task<(bool ok, LoginResponse? resp, string? error)> AuthenticateAsync(
            LoginRequest request,
            CancellationToken ct = default)
        {
            var user = await _store.FindByUsernameAsync(request.Username, ct);
            if (user is null)
                return (false, null, "Usuário não encontrado.");

            // DEMO: em produção, use senha com hash + comparação segura
            if (!string.Equals(user.password, request.Password, StringComparison.Ordinal))
                return (false, null, "Senha inválida.");

            var sessionId = Guid.NewGuid().ToString("N");

            var resp = new LoginResponse
            {
                Sub = user.sub,
                Username = user.username,
                Roles = user.roles,
                Email = user.email,
                Claims = user.claims,
                SessionId = sessionId,
                DisplayName = user.preferred_username
                // NÃO gere token aqui; o JWT será criado no endpoint/controller
            };

            // grava o usuário autenticado na sessão
            _sessionUser.SetCurrentUser(resp);

            return (true, resp, null);
        }
    }
}
