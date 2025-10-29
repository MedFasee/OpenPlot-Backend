using System;

public interface ILabelService
{
    string BuildLabel(DateTime fromUtc, DateTime toUtc, int selectRate, string source, string? terminalId);
}

public sealed class LabelService : ILabelService
{
    private readonly ITimeService _time;
    public LabelService(ITimeService time) => _time = time;

    public string BuildLabel(DateTime fromUtc, DateTime toUtc, int selectRate, string source, string? terminalId)
    {
        var (fromLocal, toLocal) = _time.ToBrazil(fromUtc, toUtc);
        var label = $"{fromLocal:yyyyMMdd}_{fromLocal:HHmmss}_{toLocal:HHmmss}_{selectRate}_{source}";
        if (!string.IsNullOrWhiteSpace(terminalId)) label += $"_{terminalId}";
        return label;
    }
}
