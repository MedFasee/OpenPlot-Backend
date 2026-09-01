using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OpenPlot.Features.Runs.Repositories;

public enum QueryPriority { Front, Background }

/// <summary>
/// Prioridade ambiente da consulta corrente. BackgroundCacheWorker marca
/// Background antes de invocar um work item; handlers HTTP nao precisam setar
/// nada (default = Front).
/// </summary>
public static class MeasurementQueryContext
{
    private static readonly AsyncLocal<QueryPriority?> Current = new();

    public static QueryPriority Priority => Current.Value ?? QueryPriority.Front;

    public static IDisposable BeginScope(QueryPriority priority)
    {
        var previous = Current.Value;
        Current.Value = priority;
        return new PriorityScope(previous);
    }

    private sealed class PriorityScope : IDisposable
    {
        private readonly QueryPriority? _previous;
        public PriorityScope(QueryPriority? previous) => _previous = previous;
        public void Dispose() => Current.Value = _previous;
    }
}

public sealed class MeasurementQuerySchedulerOptions
{
    // Sem limite fixo ainda: apenas instrumentacao (logs de espera/ocupacao).
    // Ajustar apos observar metricas reais de producao.
    public int MaxConcurrentFrontQueries { get; set; } = int.MaxValue;
    public int MaxConcurrentBackgroundQueries { get; set; } = int.MaxValue;
}

/// <summary>
/// Ponto unico de controle de concorrencia para leituras pesadas de
/// measurements, distinguindo FRONT (interativo) de BACKGROUND (preenchimento
/// de cache). Nao deve ser criado um SemaphoreSlim dentro de um handler.
/// </summary>
public interface IMeasurementQueryScheduler
{
    Task<TResult> ScheduleAsync<TResult>(
        QueryPriority priority,
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken ct);
}

public sealed class MeasurementQueryScheduler : IMeasurementQueryScheduler
{
    private readonly SemaphoreSlim _front;
    private readonly SemaphoreSlim _background;
    private readonly ILogger<MeasurementQueryScheduler> _logger;

    public MeasurementQueryScheduler(
        IOptions<MeasurementQuerySchedulerOptions> options,
        ILogger<MeasurementQueryScheduler> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _front = new SemaphoreSlim(options.Value.MaxConcurrentFrontQueries);
        _background = new SemaphoreSlim(options.Value.MaxConcurrentBackgroundQueries);
    }

    public async Task<TResult> ScheduleAsync<TResult>(
        QueryPriority priority,
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken ct)
    {
        var gate = priority == QueryPriority.Front ? _front : _background;

        var waitWatch = Stopwatch.StartNew();
        await gate.WaitAsync(ct);
        waitWatch.Stop();

        _logger.LogDebug(
            "[QUERY-SCHED][{Priority}][ACQUIRED] waitMs={WaitMs} availableFront={AvailableFront} availableBackground={AvailableBackground}",
            priority,
            waitWatch.ElapsedMilliseconds,
            _front.CurrentCount,
            _background.CurrentCount);

        try
        {
            return await operation(ct);
        }
        finally
        {
            gate.Release();
        }
    }
}
