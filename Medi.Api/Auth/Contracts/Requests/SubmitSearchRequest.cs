namespace OpenPlot.Auth.Contracts.Requests;

public sealed class SubmitSearchRequest
{
    // 1) credenciais (por agora vem no corpo; depois virá das claims do Keycloak)
    public required string Username { get; init; }

    // 2) versão de configuração do sistema (mapeia o CurrentConfig)
    public required string ConfigVersion { get; init; }

    // 3) terminais solicitados (IDs/nomes canônicos)
    public required IReadOnlyList<string> Terminais { get; init; }

    // 4) resolução pedida pela UI
    public required ResolutionDto Resolucao { get; init; }
}

public sealed class ResolutionDto
{
    // Use apenas um: Agg OU SelectRate
    public string? Agg { get; init; }       // "raw" | "20ms" | "100ms" | "1s"
    public int? SelectRate { get; init; }   // amostras/segundo (e.g., 60)
}
