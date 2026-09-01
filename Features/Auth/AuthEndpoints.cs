using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Services;
using OpenPlot.Auth.Web.Session;

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

    public static IEndpointRouteBuilder MapAuth(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/auth")
            .WithTags("Auth");

        group.MapGet("/provider", (IOptions<AuthProviderOptions> providerOptions) =>
        {
            var provider = providerOptions.Value;
            return Results.Ok(new
            {
                provider = provider.Current,
                supported = new[] { "OpenPlot", "Ons" }
            });
        });

        group.MapPost("/login", async (
            [FromBody] LoginRequest req,
            IServiceProvider services,
            IOpenPlotLoginTokenService loginTokenService,
            ISessionUserService session,
            IOptions<AuthProviderOptions> providerOptions,
            CancellationToken ct) =>
        {
            if (!providerOptions.Value.IsOpenPlot)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    title: "Fluxo de autenticação indisponível",
                    detail: "Autenticação local do OpenPlot desabilitada. Use o fluxo ONS/SSO.");
            }

            // Resolve somente após validar o provider. Assim, produção ONS não depende do users.local.json.
            var auth = services.GetRequiredService<IAuthService>();
            var (ok, resp, error) = await auth.AuthenticateAsync(req, ct);

            if (!ok || resp is null)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status401Unauthorized,
                    title: "Falha de autenticação",
                    detail: error);
            }

            session.SetCurrentUser(resp);
            var token = loginTokenService.CreateJwt(resp);

            var envelope = new ApiResponse<LoginEnvelope>
            {
                Status = StatusCodes.Status200OK,
                Data = loginTokenService.CreateEnvelope(resp, token)
            };

            return Results.Ok(envelope);
        })
        .Produces<ApiResponse<LoginEnvelope>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status401Unauthorized)
        .Produces(StatusCodes.Status409Conflict);

        group.MapPost("/logout", (
            ISessionUserService session,
            IOptions<JwtOptions> jwtOptions,
            HttpContext http) =>
        {
            var hadSession = session.GetCurrentUser() is not null;
            if (hadSession)
                session.Clear();

            var cookieName = string.IsNullOrWhiteSpace(jwtOptions.Value.CookieName)
                ? "AuthToken"
                : jwtOptions.Value.CookieName;

            http.Response.Cookies.Delete(cookieName);

            return Results.Ok(new
            {
                message = hadSession || http.User?.Identity?.IsAuthenticated == true
                    ? "Sessão encerrada"
                    : "Sessão não estava ativa"
            });
        });

        return app;
    }
}
