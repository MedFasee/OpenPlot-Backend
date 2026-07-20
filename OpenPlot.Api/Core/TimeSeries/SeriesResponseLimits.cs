using System;

namespace OpenPlot.Core.TimeSeries;

public static class SeriesResponseLimits
{
    public const string MaxResponseBytesEnvVar = "OPENPLOT_SERIES_MAX_RESPONSE_BYTES";
    public const int DefaultMaxResponseBytes = 2 * 1024 * 1024;

    public static int ResolveMaxResponseBytes()
    {
        var raw = Environment.GetEnvironmentVariable(MaxResponseBytesEnvVar);
        return int.TryParse(raw, out var configured) && configured > 0
            ? configured
            : DefaultMaxResponseBytes;
    }
}