using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Features.Runs.Services;
using Microsoft.VSDiagnostics;

namespace OpenPlot.Benchmarks;
[CPUUsageDiagnoser]
public sealed class WarmUpBenchmarks
{
    private ServiceProvider? _provider;
    private MeasurementsWarmUpService? _warmUpService;
    private IMeasurementsWarmUpQueue? _queue;
    private Guid _runId;
    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        services.AddOpenPlotApiServicesForBenchmark();
        _provider = services.BuildServiceProvider();
        _warmUpService = _provider.GetRequiredService<MeasurementsWarmUpService>();
        _queue = _provider.GetRequiredService<IMeasurementsWarmUpQueue>();
        _runId = Guid.Parse("2bf13e0a-f981-4837-8391-90f30c626f40");
    }

    [Benchmark]
    public bool EnqueueWarmUp()
    {
        return _queue!.TryEnqueue(_runId);
    }
}

public static class BenchmarkServiceCollectionExtensions
{
    public static IServiceCollection AddOpenPlotApiServicesForBenchmark(this IServiceCollection services)
    {
        return services;
    }
}