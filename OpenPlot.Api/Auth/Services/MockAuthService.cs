using System;
using System.Threading;
using System.Threading.Tasks;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Infrastructure.Auth; // IUserStore
using OpenPlot.Auth.Infrastructure.Auth.Models;

namespace OpenPlot.Auth.Services
{
    public sealed class MockAuthService : IAuthService
    {
        private readonly IUserStore _store;

        public MockAuthService(IUserStore store) => _store = store;

        public async Task<(bool ok, LoginResponse? resp, string? error)> AuthenticateAsync(
            LoginRequest request, CancellationToken ct)
        {
            var user = await _store.FindByUsernameAsync(request.Username, ct);
            if (user is null)
                return (false, null, "Usuário não encontrado.");

            // MOCK: comparação direta (em produção use hash/timing-safe)
            if (!string.Equals(user.password, request.Password, StringComparison.Ordinal))
                return (false, null, "Senha inválida.");

            var resp = new LoginResponse
            {
                Sub = user.sub,
                Username = user.username,
                Roles = user.roles,
                Email = user.email,
                Claims = user.claims,
                SessionId = Guid.NewGuid().ToString("N")
            };
            return (true, resp, null);
        }
    }
}
