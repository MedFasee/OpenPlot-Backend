using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Web.Session;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace OpenPlot.Features.Auth;

public static class AuthEndpoints
{
    public sealed class JwtOptions
    {
        public string Issuer { get; init; } = default!;
        public string Audience { get; init; } = default!;
        public string SigningKey { get; init; } = default!;
        public int ExpirationHours { get; init; } = 24;
        public string? CookieName { get; set; } = "AuthToken";
    }

    // alterado o nome da extensão
    public static IEndpointRouteBuilder MapAuth(this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("/auth")
                     .WithTags("Auth");

        // POST /api/v1/auth/login
        grp.MapGet("/provider", (IOptions<AuthProviderOptions> providerOptions) =>
        {
            var provider = providerOptions.Value;
            return Results.Ok(new
            {
                provider = provider.Current,
                supported = new[] { "OpenPlot", "Ons" }
            });
        });

        grp.MapPost("/login",
            async ([FromBody] LoginRequest req,
                   IAuthService auth,
                   IOpenPlotLoginTokenService loginTokenService,
                   ISessionUserService session,
                   IOptions<AuthProviderOptions> providerOptions,
                   HttpContext http,
                   CancellationToken ct) =>
            {
                if (!providerOptions.Value.IsOpenPlot)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status409Conflict,
                        title: "Fluxo de autenticação indisponível",
                        detail: "Autenticação local do OpenPlot desabilitada. Use o fluxo ONS/SSO.");
                }

                var (ok, resp, error) = await auth.AuthenticateAsync(req, ct);
                if (!ok || resp is null)
                    return Results.Problem(statusCode: StatusCodes.Status401Unauthorized,
                                           title: "Falha de autenticação",
                                           detail: error);

                session.SetCurrentUser(resp);
                var tokenStr = loginTokenService.CreateJwt(resp);
                var envelope = new ApiResponse<LoginEnvelope>
                {
                    Status = StatusCodes.Status200OK,
                    Data = loginTokenService.CreateEnvelope(resp, tokenStr)
                };

                return Results.Ok(envelope);
            })
        .Produces<ApiResponse<LoginEnvelope>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status401Unauthorized);

        // POST /api/v1/auth/logout
        grp.MapPost("/logout", (ISessionUserService session,
                        IOptions<JwtOptions> jwtOpt,
                        HttpContext http) =>
        {
            // Stateless logout support: when frontend authenticates directly with ONS token,
            // there may be no server session. In this case, just clear auth cookie and return OK.
            var isAuthenticated = http.User?.Identity?.IsAuthenticated == true;
            var hadSession = session.GetCurrentUser() is not null;

            if (hadSession)
                session.Clear();

            var jwt = jwtOpt.Value;
            var cookieName = string.IsNullOrWhiteSpace(jwt.CookieName)
                ? "AuthToken"
                : jwt.CookieName;

            http.Response.Cookies.Delete(cookieName);

            if (!isAuthenticated && !hadSession)
                return Results.Ok(new { message = "Sessão não estava ativa" });

            return Results.Ok(new { message = "Sessão encerrada" });
        });

        return app;
    }
}
