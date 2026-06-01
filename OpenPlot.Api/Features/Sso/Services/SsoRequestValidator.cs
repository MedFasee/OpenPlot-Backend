using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;

namespace OpenPlot.Features.Sso.Services;

public interface ISsoRequestValidator
{
    SsoRequestValidationResult Validate(
        string routeClientId,
        string? headerClientId,
        string? timestamp,
        string? nonce,
        string? signature,
        string rawBody,
        string requestPath,
        string httpMethod);
}

public sealed record SsoRequestValidationResult(
    bool IsValid,
    string? Error,
    string ClientId,
    string Nonce,
    DateTimeOffset TimestampUtc)
{
    public static SsoRequestValidationResult Fail(string? error)
        => new(false, error, string.Empty, string.Empty, default);

    public static SsoRequestValidationResult Success(string clientId, string nonce, DateTimeOffset timestampUtc)
        => new(true, null, clientId, nonce, timestampUtc);
}

public sealed class SsoRequestValidator : ISsoRequestValidator
{
    private readonly ISsoClientRegistry _clientRegistry;
    private readonly IOptions<SsoOptions> _options;

    public SsoRequestValidator(ISsoClientRegistry clientRegistry, IOptions<SsoOptions> options)
    {
        _clientRegistry = clientRegistry;
        _options = options;
    }

    public SsoRequestValidationResult Validate(
        string routeClientId,
        string? headerClientId,
        string? timestamp,
        string? nonce,
        string? signature,
        string rawBody,
        string requestPath,
        string httpMethod)
    {
        if (string.IsNullOrWhiteSpace(routeClientId))
            return SsoRequestValidationResult.Fail("ClientId da rota não informado.");

        if (string.IsNullOrWhiteSpace(headerClientId))
            return SsoRequestValidationResult.Fail("Header X-Client-Id não informado.");

        if (!string.Equals(routeClientId, headerClientId, StringComparison.OrdinalIgnoreCase))
            return SsoRequestValidationResult.Fail("ClientId da rota difere do header X-Client-Id.");

        if (!_clientRegistry.TryGetClient(routeClientId, out var client))
            return SsoRequestValidationResult.Fail("Cliente SSO não cadastrado.");

        if (string.IsNullOrWhiteSpace(client.Secret))
            return SsoRequestValidationResult.Fail("Cliente SSO sem secret configurado.");

        if (string.IsNullOrWhiteSpace(timestamp)
            || !DateTimeOffset.TryParse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var timestampUtc))
        {
            return SsoRequestValidationResult.Fail("Header X-Timestamp inválido.");
        }

        if (string.IsNullOrWhiteSpace(nonce))
            return SsoRequestValidationResult.Fail("Header X-Nonce não informado.");

        if (string.IsNullOrWhiteSpace(signature))
            return SsoRequestValidationResult.Fail("Header X-Signature não informado.");

        var requestAge = (DateTimeOffset.UtcNow - timestampUtc.ToUniversalTime()).Duration();
        if (requestAge > TimeSpan.FromSeconds(Math.Max(_options.Value.RequestTtlSeconds, 30)))
            return SsoRequestValidationResult.Fail("Timestamp do request SSO expirado.");

        var canonical = BuildCanonicalMessage(httpMethod, requestPath, timestampUtc, nonce, rawBody);
        var expectedSignature = ComputeHmac(client.Secret, canonical);

        if (!MatchesSignature(expectedSignature, signature))
            return SsoRequestValidationResult.Fail("Assinatura HMAC inválida.");

        return SsoRequestValidationResult.Success(client.ClientId, nonce, timestampUtc.ToUniversalTime());
    }

    public static string BuildCanonicalMessage(
        string httpMethod,
        string requestPath,
        DateTimeOffset timestampUtc,
        string nonce,
        string rawBody)
    {
        var bodyHash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(rawBody ?? string.Empty)));
        return string.Join('\n',
            httpMethod.ToUpperInvariant(),
            requestPath,
            timestampUtc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            nonce,
            bodyHash);
    }

    private static byte[] ComputeHmac(string secret, string canonicalMessage)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        return hmac.ComputeHash(Encoding.UTF8.GetBytes(canonicalMessage));
    }

    private static bool MatchesSignature(byte[] expectedSignature, string providedSignature)
    {
        if (TryDecodeBase64(providedSignature, out var providedBytes)
            || TryDecodeHex(providedSignature, out providedBytes))
        {
            return CryptographicOperations.FixedTimeEquals(expectedSignature, providedBytes);
        }

        return false;
    }

    private static bool TryDecodeBase64(string value, out byte[] bytes)
    {
        try
        {
            bytes = Convert.FromBase64String(value);
            return true;
        }
        catch (FormatException)
        {
            bytes = [];
            return false;
        }
    }

    private static bool TryDecodeHex(string value, out byte[] bytes)
    {
        try
        {
            bytes = Convert.FromHexString(value);
            return true;
        }
        catch (FormatException)
        {
            bytes = [];
            return false;
        }
    }
}
