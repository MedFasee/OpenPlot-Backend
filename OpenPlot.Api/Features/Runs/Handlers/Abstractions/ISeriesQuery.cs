namespace OpenPlot.Features.Runs.Handlers.Abstractions;

/// <summary>
/// Interface comum para queries de séries temporais.
/// Padroniza propriedades e comportamentos compartilhados.
/// </summary>
public interface ISeriesQuery
{
    /// <summary>
    /// ID do Run a ser consultado.
    /// </summary>
    Guid RunId { get; }

    /// <summary>
    /// Parâmetro de downsampling ("all" ou número inteiro).
    /// </summary>
    string? MaxPoints { get; }

    /// <summary>
    /// Indica se o downsampling deve ser desabilitado.
    /// </summary>
    bool MaxPointsIsAll { get; }

    /// <summary>
    /// Resolve o número máximo de pontos a retornar.
    /// </summary>
    /// <param name="default">Valor padrão caso MaxPoints seja nulo.</param>
    /// <returns>Número máximo de pontos (int.MaxValue se "all").</returns>
    int ResolveMaxPoints(int @default = 5000);
}
