using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace OpenPlot.Features.Runs.Repositories;

public interface IQueryExecutionCoordinator
{
    /// <summary>
    /// Deduplica leituras identicas de measurements em voo (MISS/HIT-INFLIGHT)
    /// e mantem o resultado bem-sucedido reutilizavel por um grace period
    /// (HIT-GRACE). Falhas nunca ficam reutilizaveis. O cancellationToken do
    /// consumidor cancela apenas a sua propria espera, nunca a leitura
    /// compartilhada.
    /// </summary>
    Task<TResult> ExecuteAsync<TKey, TResult>(
        string operation,
        TKey key,
        Func<CancellationToken, Task<TResult>> factory,
        CancellationToken consumerCancellationToken)
        where TKey : notnull;
}

// Infraestrutura unica de deduplicacao de leituras usada por todas as familias
// de series (Simple/Phasor/AngleFrames/PowerFrames). TKey continua fortemente
// tipado por familia (SimpleQueryKey, PhasorQueryKey, ...): este servico apenas
// mantem um dicionario interno por par (TKey, TResult), nunca uma chave/valor
// genericos compartilhados entre familias.
public sealed class QueryExecutionCoordinator : IQueryExecutionCoordinator
{
    private static readonly TimeSpan GracePeriod = TimeSpan.FromSeconds(2);

    private readonly ConcurrentDictionary<(Type KeyType, Type ResultType), object> _dictionaries = new();
    private readonly ILogger<QueryExecutionCoordinator> _logger;

    public QueryExecutionCoordinator(ILogger<QueryExecutionCoordinator> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<TResult> ExecuteAsync<TKey, TResult>(
        string operation,
        TKey key,
        Func<CancellationToken, Task<TResult>> factory,
        CancellationToken consumerCancellationToken)
        where TKey : notnull
    {
        var dictionary = GetDictionary<TKey, TResult>();

        Lazy<Task<TResult>>? candidate = null;
        var lazy = dictionary.GetOrAdd(
            key,
            _ =>
            {
                candidate = new Lazy<Task<TResult>>(
                    () => RunAndScheduleRemovalAsync(dictionary, key, factory),
                    LazyThreadSafetyMode.ExecutionAndPublication);

                return candidate;
            });

        var createdByThisCall = ReferenceEquals(lazy, candidate);
        var status = createdByThisCall
            ? "MISS"
            : lazy.Value.IsCompleted ? "HIT-GRACE" : "HIT-INFLIGHT";

        _logger.LogInformation(
            "[QUERY-COORD][{Operation}][COALESCE] key={Key} shared={Shared}",
            operation,
            key,
            status);

        return await lazy.Value.WaitAsync(consumerCancellationToken);
    }

    private static async Task<TResult> RunAndScheduleRemovalAsync<TKey, TResult>(
        ConcurrentDictionary<TKey, Lazy<Task<TResult>>> dictionary,
        TKey key,
        Func<CancellationToken, Task<TResult>> factory)
        where TKey : notnull
    {
        try
        {
            // A leitura compartilhada roda ate o fim independentemente do
            // consumidor original ter desistido: CancellationToken.None e
            // proposital aqui.
            var result = await factory(CancellationToken.None);

            _ = RemoveAfterGraceAsync(dictionary, key);

            return result;
        }
        catch
        {
            dictionary.TryRemove(key, out _);
            throw;
        }
    }

    private static async Task RemoveAfterGraceAsync<TKey, TResult>(
        ConcurrentDictionary<TKey, Lazy<Task<TResult>>> dictionary,
        TKey key)
        where TKey : notnull
    {
        await Task.Delay(GracePeriod);
        dictionary.TryRemove(key, out _);
    }

    private ConcurrentDictionary<TKey, Lazy<Task<TResult>>> GetDictionary<TKey, TResult>()
        where TKey : notnull
    {
        return (ConcurrentDictionary<TKey, Lazy<Task<TResult>>>)_dictionaries.GetOrAdd(
            (typeof(TKey), typeof(TResult)),
            _ => new ConcurrentDictionary<TKey, Lazy<Task<TResult>>>());
    }
}
