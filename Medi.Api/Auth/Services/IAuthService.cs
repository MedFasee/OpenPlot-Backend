using System.Threading;
using System.Threading.Tasks;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Contracts.Responses;

namespace OpenPlot.Auth.Services
{
    public interface IAuthService
    {
        Task<(bool ok, LoginResponse? resp, string? error)> AuthenticateAsync(
            LoginRequest request, CancellationToken ct = default);
    }
}
