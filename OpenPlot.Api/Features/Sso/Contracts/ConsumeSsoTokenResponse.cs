using OpenPlot.Auth.Contracts.Responses;

namespace OpenPlot.Features.Sso.Contracts;

public sealed class ConsumeSsoTokenResponse
{
    public required string ConsultaId { get; init; }
    public required string OriginClient { get; init; }
    public required string RedirectPath { get; init; }
    public required LoginEnvelope Login { get; init; }
}
