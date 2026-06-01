using Microsoft.Extensions.Options;

namespace OpenPlot.Features.Sso.Services;

public interface ISsoClientRegistry
{
    bool TryGetClient(string clientId, out SsoClientOptions client);
}

public sealed class SsoClientRegistry : ISsoClientRegistry
{
    private readonly IReadOnlyDictionary<string, SsoClientOptions> _clients;

    public SsoClientRegistry(IOptions<List<SsoClientOptions>> options)
    {
        _clients = (options.Value ?? [])
            .Where(static client => !string.IsNullOrWhiteSpace(client.ClientId))
            .ToDictionary(client => client.ClientId, StringComparer.OrdinalIgnoreCase);
    }

    public bool TryGetClient(string clientId, out SsoClientOptions client)
        => _clients.TryGetValue(clientId, out client!);
}
