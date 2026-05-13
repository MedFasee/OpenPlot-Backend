namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed class IngestorRuntimeContext
{
    internal IngestorRuntimeContext(IngestorOptions options)
    {
        Options = options;
    }

    internal IngestorOptions Options { get; }
}
