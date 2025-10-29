using OpenPlot.Auth.Infrastructure.Auth.Models;

namespace OpenPlot.Auth.Infrastructure.Auth;

public interface IUserStore
{
    Task<UserRecord?> FindByUsernameAsync(string username, CancellationToken ct = default);
    Task<UserRecord?> FindBySubAsync(string sub, CancellationToken ct = default);
}

