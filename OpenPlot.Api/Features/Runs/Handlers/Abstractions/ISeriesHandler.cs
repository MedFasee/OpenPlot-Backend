using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers.Abstractions;

/// <summary>
/// Interface para handler genérico de séries temporais.
/// Define contrato que todos os handlers específicos devem respeitar.
/// </summary>
/// <typeparam name="TQuery">Tipo de query específico do handler.</typeparam>
public interface ISeriesHandler<TQuery>
    where TQuery : ISeriesQuery
{
    /// <summary>
    /// Executa o processamento completo de uma requisição de série.
    /// </summary>
    /// <param name="query">Query com parâmetros da requisição.</param>
    /// <param name="window">Janela temporal (from/to).</param>
    /// <param name="modes">Modos de UI (opcional).</param>
    /// <param name="ct">Token de cancelamento.</param>
    /// <returns>Resultado HTTP (Ok, NotFound, BadRequest, etc).</returns>
    Task<IResult> HandleAsync(
        TQuery query,
        WindowQuery window,
        Dictionary<string, object?>? modes,
        CancellationToken ct);
}

/// <summary>
/// Interface para serviço de cache de séries analisadas.
/// Permite persistência para consultas subsequentes.
/// </summary>
public interface ISeriesCacheService
{
    /// <summary>
    /// Salva série processada em cache para recuperação posterior.
    /// </summary>
    /// <param name="runId">ID do run associado.</param>
    /// <param name="payload">Dados de série a cachear.</param>
    /// <param name="ct">Token de cancelamento.</param>
    /// <returns>ID do cache gerado (pode ser string ou guid).</returns>
    Task<object?> SaveAsync(
        Guid runId,
        RowsCacheV2 payload,
        CancellationToken ct);
}
