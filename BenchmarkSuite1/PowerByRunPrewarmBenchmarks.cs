using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Microsoft.VSDiagnostics;

namespace BenchmarkSuite1;
[SimpleJob(RuntimeMoniker.Net10_0)]
[CPUUsageDiagnoser]
public sealed class PowerByRunPrewarmBenchmarks
{
    [Params(false, true)]
    public bool UsePgPrewarm { get; set; }

    [Params("2bf13e0a-f981-4837-8391-90f30c626f40")]
    public string RunId { get; set; } = string.Empty;

    [Benchmark]
    public bool ExecutePowerRequestShape()
    {
        return UsePgPrewarm ? RunId.Length > 0 : RunId.Length >= 0;
    }
}