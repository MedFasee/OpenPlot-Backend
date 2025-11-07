using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Web.Session;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace OpenPlot.Features.Auth;

public static class AuthEndpoints
{
    public sealed class JwtOptions
    {
        public string Issuer { get; init; } = default!;
        public string Audience { get; init; } = default!;
        public string SigningKey { get; init; } = default!;
        public int ExpirationHours { get; init; } = 8;
    }

    // alterado o nome da extensão
    public static IEndpointRouteBuilder MapAuth(this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("/auth")
                     .WithTags("Auth");

        // POST /api/v1/auth/login
        grp.MapPost("/login",
            async ([FromBody] LoginRequest req,
                   IAuthService auth,
                   ISessionUserService session,
                   IOptions<JwtOptions> jwtOpt,
                   CancellationToken ct) =>
            {
                var (ok, resp, error) = await auth.AuthenticateAsync(req, ct);
                if (!ok || resp is null)
                    return Results.Problem(statusCode: StatusCodes.Status401Unauthorized,
                                           title: "Falha de autenticação",
                                           detail: error);

                session.SetCurrentUser(resp);

                var now = DateTime.UtcNow;
                var jwt = jwtOpt.Value;

                var claims = new List<Claim>
                {
                    new(JwtRegisteredClaimNames.Sub, resp.Sub),
                    new(JwtRegisteredClaimNames.UniqueName, resp.Username),
                    new(JwtRegisteredClaimNames.Email, resp.Email ?? string.Empty),
                    new("sid", resp.SessionId ?? Guid.NewGuid().ToString("N"))
                };

                if (resp.Roles is not null)
                    claims.AddRange(resp.Roles.Select(r => new Claim(ClaimTypes.Role, r)));

                var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwt.SigningKey));
                var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

                var token = new JwtSecurityToken(
                    issuer: jwt.Issuer,
                    audience: jwt.Audience,
                    claims: claims,
                    notBefore: now,
                    expires: now.AddHours(jwt.ExpirationHours),
                    signingCredentials: creds
                );

                var tokenStr = new JwtSecurityTokenHandler().WriteToken(token);

                static string MapRole(IReadOnlyCollection<string>? roles)
                {
                    if (roles?.Contains("admin", StringComparer.OrdinalIgnoreCase) == true) return "admin";
                    if (roles?.Contains("editor", StringComparer.OrdinalIgnoreCase) == true) return "editor";
                    return "reader";
                }

                var envelope = new ApiResponse<LoginEnvelope>
                {
                    Status = StatusCodes.Status200OK,
                    Data = new LoginEnvelope
                    {
                        Token = tokenStr,
                        Usuario = new UsuarioDto
                        {
                            Nome = resp.DisplayName ?? resp.Username,
                            Email = resp.Email ?? $"{resp.Username}@medplot.com",
                            Role = MapRole(resp.Roles)
                        }
                    }
                };

                return Results.Ok(envelope);
            })
        .Produces<ApiResponse<LoginEnvelope>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status401Unauthorized);

        // POST /api/v1/auth/logout
        grp.MapPost("/logout", (ISessionUserService session) =>
        {
            var user = session.GetCurrentUser();
            if (user is null)
                return Results.Unauthorized();

            session.Clear();
            return Results.Ok(new { message = "Sessão encerrada" });
        })
        .RequireAuthorization();

        // GET /api/v1/auth/me
        grp.MapGet("/me", (ISessionUserService session) =>
        {
            var user = session.GetCurrentUser();
            return user is not null ? Results.Ok(user) : Results.Unauthorized();
        })
        .RequireAuthorization();

        return app;
    }
}
