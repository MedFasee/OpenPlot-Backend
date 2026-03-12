using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers.Abstractions;

/// <summary>
/// Interface para validação de parâmetros específicos do domínio de séries.
/// Centraliza regras de negócio de validação.
/// </summary>
public interface ISeriesValidator
{
    /// <summary>
    /// Valida parâmetros e retorna resultado com mensagem de erro se houver.
    /// </summary>
    /// <returns>
    /// Tupla contendo:
    /// - isValid: true se validação passou
    /// - errorMessage: mensagem de erro (null se válido)
    /// </returns>
    (bool isValid, string? errorMessage) Validate();
}

/// <summary>
/// Interface para transformação de dados brutos em séries temporais formatadas.
/// Encapsula lógica de downsampling, agrupamento e formatação.
/// </summary>
public interface ISeriesDataTransformer
{
    /// <summary>
    /// Transforma linhas de medição em séries temporais processadas.
    /// </summary>
    /// <param name="rows">Linhas de medição brutos do repositório.</param>
    /// <param name="maxPoints">Máximo de pontos após downsampling.</param>
    /// <param name="noDownsample">Se true, desabilita downsampling.</param>
    /// <returns>Lista de séries formatadas para retorno ao cliente.</returns>
    List<object> Transform(
        IReadOnlyList<MeasurementRow> rows,
        int maxPoints,
        bool noDownsample);
}

/// <summary>
/// Interface para construção da resposta HTTP padronizada.
/// Garante consistência entre endpoints.
/// </summary>
public interface ISeriesResponseBuilder
{
    /// <summary>
    /// Constrói resposta completa com metadados, janela temporal e séries.
    /// </summary>
    /// <param name="runId">ID do run processado.</param>
    /// <param name="series">Séries formatadas para resposta.</param>
    /// <param name="windowFrom">Timestamp inicial da janela.</param>
    /// <param name="windowTo">Timestamp final da janela.</param>
    /// <param name="meta">Metadados do gráfico (título, labels, etc).</param>
    /// <param name="rows">Linhas de medição para extrair informações resolução.</param>
    /// <param name="cacheId">ID do cache salvo (opcional).</param>
    /// <param name="modes">UI modes (opcional).</param>
    /// <returns>Objeto anônimo com estrutura de resposta padronizada.</returns>
    object BuildResponse(
        Guid runId,
        List<object> series,
        DateTime windowFrom,
        DateTime windowTo,
        PlotMetaDto meta,
        IReadOnlyList<MeasurementRow> rows,
        string? cacheId = null,
        Dictionary<string, object?>? modes = null);
}
