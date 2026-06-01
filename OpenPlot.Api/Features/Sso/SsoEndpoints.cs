using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Web.Session;
using OpenPlot.Features.Auth;
using OpenPlot.Features.Sso.Contracts;
using OpenPlot.Features.Sso.Repositories;
using OpenPlot.Features.Sso.Services;

namespace OpenPlot.Features.Sso;

public static class SsoEndpoints
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public static IEndpointRouteBuilder MapSso(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/sso")
            .WithTags("SSO");

        group.MapPost("/{clientId}/link",
            async ([FromRoute] string clientId,
                HttpRequest request,
                ISsoRequestValidator validator,
                ISsoAuthRepository repository,
                IOptions<SsoOptions> ssoOptions,
                CancellationToken ct) =>
            {
                var rawBody = await ReadRawBodyAsync(request, ct);
                var validation = validator.Validate(
                    clientId,
                    request.Headers[SsoHttpHeaders.ClientId].FirstOrDefault(),
                    request.Headers[SsoHttpHeaders.Timestamp].FirstOrDefault(),
                    request.Headers[SsoHttpHeaders.Nonce].FirstOrDefault(),
                    request.Headers[SsoHttpHeaders.Signature].FirstOrDefault(),
                    rawBody,
                    request.Path.Value ?? string.Empty,
                    request.Method);

                if (!validation.IsValid)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status401Unauthorized,
                        title: "Request SSO inválido",
                        detail: validation.Error);
                }

                var payload = JsonSerializer.Deserialize<CreateSsoLinkRequest>(rawBody, SerializerOptions);
                if (payload is null || string.IsNullOrWhiteSpace(payload.ConsultaId))
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Payload SSO inválido",
                        detail: "O campo consultaId é obrigatório.");
                }

                var createdAtUtc = DateTime.UtcNow;
                var nonceAccepted = await repository.TryRegisterNonceAsync(
                    validation.ClientId,
                    validation.Nonce,
                    createdAtUtc,
                    createdAtUtc.AddSeconds(Math.Max(ssoOptions.Value.RequestTtlSeconds, 30)),
                    ct);

                if (!nonceAccepted)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status409Conflict,
                        title: "Nonce já utilizado",
                        detail: "A requisição SSO já foi processada anteriormente.");
                }

                var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
                var expiresAtUtc = createdAtUtc.AddSeconds(Math.Max(ssoOptions.Value.LoginTokenTtlSeconds, 30));

                await repository.CreateLoginTokenAsync(token, payload.ConsultaId, validation.ClientId, createdAtUtc, expiresAtUtc, ct);

                var response = new ApiResponse<CreateSsoLinkResponse>
                {
                    Status = StatusCodes.Status200OK,
                    Data = new CreateSsoLinkResponse
                    {
                        ClientId = validation.ClientId,
                        ConsultaId = payload.ConsultaId,
                        Url = BuildConsumeUrl(request, ssoOptions.Value, token),
                        ExpiresAtUtc = expiresAtUtc
                    }
                };

                return Results.Ok(response);
            })
            .Produces<ApiResponse<CreateSsoLinkResponse>>(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status401Unauthorized)
            .Produces(StatusCodes.Status409Conflict);

        group.MapPost("/consumir-token",
            async ([FromBody] ConsumeSsoTokenRequest payload,
                ISsoAuthRepository repository,
                ISsoIdentityService identityService,
                IOpenPlotLoginTokenService loginTokenService,
                ISessionUserService sessionUserService,
                IOptions<SsoOptions> ssoOptions,
                CancellationToken ct) =>
            {
                if (string.IsNullOrWhiteSpace(payload.Token))
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Token SSO inválido",
                        detail: "O token temporário é obrigatório.");
                }

                var consumedToken = await repository.ConsumeLoginTokenAsync(payload.Token, ct);
                if (consumedToken is null)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status401Unauthorized,
                        title: "Token SSO inválido",
                        detail: "O token temporário não existe, expirou ou já foi utilizado.");
                }

                var (ok, loginResponse, error) = await identityService.ResolveAsync(consumedToken.OriginClient, consumedToken.ConsultaId, ct);
                if (!ok || loginResponse is null)
                {
                    return Results.Problem(
                        statusCode: StatusCodes.Status401Unauthorized,
                        title: "Usuário técnico SSO inválido",
                        detail: error);
                }

                sessionUserService.SetCurrentUser(loginResponse);

                var jwt = loginTokenService.CreateJwt(loginResponse);
                var response = new ApiResponse<ConsumeSsoTokenResponse>
                {
                    Status = StatusCodes.Status200OK,
                    Data = new ConsumeSsoTokenResponse
                    {
                        ConsultaId = consumedToken.ConsultaId,
                        OriginClient = consumedToken.OriginClient,
                        RedirectPath = BuildRedirectPath(ssoOptions.Value, consumedToken.ConsultaId),
                        Login = loginTokenService.CreateEnvelope(loginResponse, jwt)
                    }
                };

                return Results.Ok(response);
            })
            .Produces<ApiResponse<ConsumeSsoTokenResponse>>(StatusCodes.Status200OK)
            .Produces(StatusCodes.Status400BadRequest)
            .Produces(StatusCodes.Status401Unauthorized);

        return app;
    }

    private static string BuildConsumeUrl(HttpRequest request, SsoOptions options, string token)
    {
        var baseUrl = string.IsNullOrWhiteSpace(options.FrontendBaseUrl)
            ? $"{request.Scheme}://{request.Host.Value}"
            : options.FrontendBaseUrl.TrimEnd('/');

        var consumePath = string.IsNullOrWhiteSpace(options.ConsumePath)
            ? "/sso/consumir"
            : options.ConsumePath;

        var separator = consumePath.Contains('?', StringComparison.Ordinal) ? '&' : '?';
        return $"{baseUrl}{consumePath}{separator}token={Uri.EscapeDataString(token)}";
    }

    private static string BuildRedirectPath(SsoOptions options, string consultaId)
    {
        var template = string.IsNullOrWhiteSpace(options.RedirectPathTemplate)
            ? "/processar-busca?consultaId={consultaId}"
            : options.RedirectPathTemplate;

        return template.Replace("{consultaId}", Uri.EscapeDataString(consultaId), StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<string> ReadRawBodyAsync(HttpRequest request, CancellationToken ct)
    {
        request.EnableBuffering();
        request.Body.Position = 0;

        using var reader = new StreamReader(request.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        var body = await reader.ReadToEndAsync(ct);
        request.Body.Position = 0;
        return body;
    }
}
