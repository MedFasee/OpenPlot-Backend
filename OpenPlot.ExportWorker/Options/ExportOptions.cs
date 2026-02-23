namespace OpenPlot.ExportWorker.Options;

public sealed class ExportOptions
{
    public string RootDir { get; set; } = "C:\\OpenPlot\\exports";
    public int PollIntervalMs { get; set; } = 1500;

    // COMTRADE 2013 (C37.111-2013 / IEC 60255-24 Ed.2)
    public string TimeCodeMode { get; set; } = "UTC";   // UTC | RUN_OFFSET
    public string TmqCode { get; set; } = "0";          // 1-char
    public string LeapSec { get; set; } = "0";          // 1-char
    public int NominalFrequencyFallback { get; set; } = 60;
    public string FileType { get; set; } = "ASCII";     // ASCII (MVP)
}