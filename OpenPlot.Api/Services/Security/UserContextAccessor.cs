using System.Security.Claims;
using OpenPlot.Auth.Web.Session;

namespace OpenPlot.Api.Services.Security;

public interface IUserContextAccessor
{
    UserContextSnapshot GetCurrent(HttpContext context);
    string? GetUsername(HttpContext context);
    string? GetUserId(HttpContext context);
}

public sealed record UserContextSnapshot(string? UserName, string? UserId);

public sealed class UserContextAccessor : IUserContextAccessor
{
    private readonly ISessionUserService _sessionUserService;

    public UserContextAccessor(ISessionUserService sessionUserService)
    {
        _sessionUserService = sessionUserService;
    }

    public UserContextSnapshot GetCurrent(HttpContext context)
    {
        string? userName = null;
        string? userId = null;

        if (context.User?.Identity?.IsAuthenticated == true)
        {
            userName =
                context.User.FindFirst("username")?.Value
                ?? context.User.FindFirst("unique_name")?.Value
                ?? context.User.Identity?.Name
                ?? context.User.FindFirst(ClaimTypes.Name)?.Value
                ?? context.User.FindFirst(ClaimTypes.Email)?.Value;

            userId =
                context.User.FindFirst("sub")?.Value
                ?? context.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        }

        if (string.IsNullOrWhiteSpace(userName))
        {
            var login = _sessionUserService.GetCurrentUser();
            if (login is not null)
            {
                userName = login.Username;
                userId ??= login.Sub ?? login.Username;
            }
        }

        return new UserContextSnapshot(userName, userId);
    }

    public string? GetUsername(HttpContext context) => GetCurrent(context).UserName;

    public string? GetUserId(HttpContext context) => GetCurrent(context).UserId;
}
