using OpenPlot.Auth.Contracts.Responses;

namespace OpenPlot.Auth.Web.Session;

public interface ISessionUserService
{
    void SetCurrentUser(LoginResponse user);
    LoginResponse? GetCurrentUser();
    void Clear();
}
