namespace OpenPlot.Features.Sso.Repositories;

public interface ISsoAuthRepository
{
    Task<bool> TryRegisterNonceAsync(string clientId, string nonce, DateTime createdAtUtc, DateTime expiresAtUtc, CancellationToken ct = default);
    Task CreateLoginTokenAsync(string token, string consultaId, string originClient, DateTime createdAtUtc, DateTime expiresAtUtc, CancellationToken ct = default);
    Task<ConsumedSsoLoginToken?> ConsumeLoginTokenAsync(string token, CancellationToken ct = default);
}

public sealed class ConsumedSsoLoginToken
{
    public Guid Id { get; init; }
    public required string ConsultaId { get; init; }
    public required string OriginClient { get; init; }
    public DateTime CreatedAtUtc { get; init; }
    public DateTime ExpiresAtUtc { get; init; }
}
