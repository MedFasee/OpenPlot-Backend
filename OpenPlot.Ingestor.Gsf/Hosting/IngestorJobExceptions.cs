using System;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed class JobCanceledException : OperationCanceledException
{
    public JobCanceledException(Guid jobId)
        : base("Consulta cancelada pelo usuário. jobId=" + jobId)
    {
    }
}
