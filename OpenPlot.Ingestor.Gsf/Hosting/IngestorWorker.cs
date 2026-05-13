using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed class IngestorWorker : BackgroundService
{
    private readonly ILogger<IngestorWorker> _logger;
    private readonly IngestorRuntimeContext _runtimeContext;
    private readonly IIngestorJobService _jobService;

    public IngestorWorker(
        ILogger<IngestorWorker> logger,
        IngestorRuntimeContext runtimeContext,
        IIngestorJobService jobService)
    {
        _logger = logger;
        _runtimeContext = runtimeContext;
        _jobService = jobService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = _runtimeContext.Options;

        _logger.LogInformation(
            "Ingestor iniciado. Db={Db} Workers={Workers} ChunksPorJob={ChunksPerJob} ChunksGlobais={GlobalChunks}",
            options.PgConnString,
            options.MaxParallelJobs,
            options.MaxParallelChunks,
            options.GlobalMaxParallelChunks);

        var workers = Enumerable.Range(1, options.MaxParallelJobs)
            .Select(workerId => _jobService.RunWorkerSlotAsync(workerId, stoppingToken))
            .ToArray();

        await Task.WhenAll(workers);
    }
}
