using System;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal interface IIngestorJobService
{
    Task RunWorkerSlotAsync(int workerId, CancellationToken stoppingToken);
}

internal sealed class IngestorJobService : IIngestorJobService
{
    private readonly IngestorRuntimeContext _runtimeContext;
    private readonly IQueuedJobPicker _queuedJobPicker;
    private readonly IIngestorJobProcessor _jobProcessor;

    public IngestorJobService(
        IngestorRuntimeContext runtimeContext,
        IQueuedJobPicker queuedJobPicker,
        IIngestorJobProcessor jobProcessor)
    {
        _runtimeContext = runtimeContext;
        _queuedJobPicker = queuedJobPicker;
        _jobProcessor = jobProcessor;
    }

    public async Task RunWorkerSlotAsync(int workerId, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var job = _queuedJobPicker.TryPickQueuedJob();
                if (job is null)
                {
                    await Task.Delay(_runtimeContext.Options.PollInterval, stoppingToken);
                    continue;
                }

                _jobProcessor.ProcessJob(job, workerId);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine("[worker " + workerId + "] " + ex.Message);
                await Task.Delay(_runtimeContext.Options.PollInterval, stoppingToken);
            }
        }
    }
}
