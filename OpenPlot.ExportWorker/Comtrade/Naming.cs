using System.Text;

namespace OpenPlot.ExportWorker.Comtrade;

public static class Naming
{
    public static string SafeFileBase(string raw, int maxLen = 80)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var sb = new StringBuilder(raw.Length);
        foreach (var ch in raw.Trim())
            sb.Append(invalid.Contains(ch) ? '_' : ch);

        var s = sb.ToString().Trim();
        if (s.Length == 0) s = "PMU";
        if (s.Length > maxLen) s = s[..maxLen];
        return s;
    }

    public static string ChannelName(string quantity, string component, string? phase)
    {
        var ph = string.IsNullOrWhiteSpace(phase) ? "" : phase.Trim();
        var name = $"{quantity}_{component}_{ph}".TrimEnd('_').ToLowerInvariant();
        return SafeToken(name, 48);
    }

    private static string SafeToken(string raw, int maxLen)
    {
        // Para channel id, evitar espaços e vírgulas
        var s = raw.Replace(' ', '_').Replace(',', '_');
        if (s.Length > maxLen) s = s[..maxLen];
        return s;
    }
}