using System.Globalization;
using System.IO.Compression;
using System.Text;
using OpenPlot.ExportWorker.Domain;

namespace OpenPlot.ExportWorker.Comtrade;

public sealed class Comtrade2013Writer
{
    private const int ScaleMax = 32767;

    public void WriteZipToStream(
        Stream stream,
        RunContext run,
        IReadOnlyList<PmuComtrade> pmus,
        int nominalFrequency,
        string timeCodeMode,
        string tmqCode,
        string leapSec,
        string fileType)
    {
        using var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true);

        // evitar colisões de nomes dentro do zip
        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var pmu in pmus)
        {
            if (pmu.Analogs.Count == 0 && pmu.Digitals.Count == 0) continue;

            var baseName = MakeUnique(used, pmu.PmuFileSafeName);
            var cfgName = baseName + ".cfg";
            var datName = baseName + ".dat";

            // Calcula A/B por canal analógico
            var analogs = pmu.Analogs
                .OrderBy(c => c.Index)
                .Select(c =>
                {
                    var (a, b) = ComputeAB(c.Values);
                    return (c, a, b);
                })
                .ToList();

            var digitals = pmu.Digitals.OrderBy(d => d.Index).ToList();

            int nSamples =
                analogs.Count > 0 ? analogs[0].c.Values.Length :
                digitals.Count > 0 ? digitals[0].Values.Length :
                0;

            WriteCfg2013(
                zip.CreateEntry(cfgName, CompressionLevel.Optimal).Open(),
                stationName: pmu.PmuDisplayName,
                nominalFrequency: nominalFrequency,
                sampleRate: pmu.SampleRate,
                nSamples: nSamples,
                startUtc: pmu.StartUtc,
                analogs: analogs,
                digitals: digitals,
                timeCodeMode: timeCodeMode,
                tmqCode: tmqCode,
                leapSec: leapSec,
                fileType: fileType
            );

            WriteDatAscii(
                zip.CreateEntry(datName, CompressionLevel.Optimal).Open(),
                sampleRate: pmu.SampleRate,
                nSamples: nSamples,
                analogs: analogs,
                digitals: digitals
            );
        }
    }

    private static void WriteCfg2013(
        Stream entryStream,
        string stationName,
        int nominalFrequency,
        int sampleRate,
        int nSamples,
        DateTimeOffset startUtc,
        List<(AnalogSeries c, double a, double b)> analogs,
        List<DigitalSeries> digitals,
        string timeCodeMode,
        string tmqCode,
        string leapSec,
        string fileType)
    {
        var inv = CultureInfo.InvariantCulture;
        using var sw = new StreamWriter(entryStream, Encoding.ASCII);

        int na = analogs.Count;
        int nd = digitals.Count;
        int tt = na + nd;

        // COMTRADE “2014” => rev_year=2013
        sw.WriteLine($"{stationName},openplot,2013");
        sw.WriteLine($"{tt},{na}A,{nd}D");

        // ANALÓGICOS
        foreach (var (c, a, b) in analogs)
        {
            // An, ch_id, ph, ccbm, uu, a, b, skew, min, max, primary, secondary, PS
            // Observação: estamos deixando ph/ccbm vazios (,,,)
            sw.WriteLine(
                $"{c.Index}," +
                $"{PadRightCsv(c.Name, 40)}" +
                $",,,{c.Unit}," +
                $"{a.ToString("G17", inv)}," +
                $"{b.ToString("G17", inv)}," +
                $"0.0,{-ScaleMax},{ScaleMax},1.0,1.0,P");
        }

        // DIGITAIS
        // Dn, ch_id, ph, ccbm, y
        foreach (var d in digitals)
        {
            // mantendo bem simples: "{idx},{name},,,0"
            sw.WriteLine($"{d.Index},{PadRightCsv(d.Name, 30)},,,0");
        }

        sw.WriteLine($"{nominalFrequency}.0");
        sw.WriteLine("1");
        sw.WriteLine($"{sampleRate},{nSamples}");

        var dt = startUtc.ToString("dd/MM/yyyy,HH:mm:ss.ffffff", inv);
        sw.WriteLine(dt);
        sw.WriteLine(dt);

        sw.WriteLine(string.Equals(fileType, "ASCII", StringComparison.OrdinalIgnoreCase) ? "ASCII" : "ASCII");
        sw.WriteLine("1.0"); // timemult

        // linhas extra do 2013
        var tc = ResolveTimeCode(startUtc, timeCodeMode);
        sw.WriteLine($"{tc},{tc}"); // time_code, local_code
        sw.WriteLine($"{Fix1(tmqCode)},{Fix1(leapSec)}"); // tmq_code, leapsec

        sw.Flush();
    }

    private static void WriteDatAscii(
        Stream entryStream,
        int sampleRate,
        int nSamples,
        List<(AnalogSeries c, double a, double b)> analogs,
        List<DigitalSeries> digitals)
    {
        using var sw = new StreamWriter(entryStream, Encoding.ASCII);

        for (int i = 0; i < nSamples; i++)
        {
            int sample = i + 1;
            long tUs = (long)Math.Round(i * 1_000_000.0 / sampleRate);

            sw.Write(sample.ToString(CultureInfo.InvariantCulture));
            sw.Write(",");
            sw.Write(tUs.ToString(CultureInfo.InvariantCulture));

            // analógicos: escreve int16 quantizado
            foreach (var (c, a, b) in analogs)
            {
                short s = Encode(c.Values[i], a, b);
                sw.Write(",");
                sw.Write(s.ToString(CultureInfo.InvariantCulture));
            }

            // digitais: 0/1
            foreach (var d in digitals)
            {
                sw.Write(",");
                sw.Write(d.Values[i] ? "1" : "0");
            }

            sw.WriteLine();
        }

        sw.Flush();
    }

    private static (double A, double B) ComputeAB(double[] y)
    {
        double min = y.Min();
        double max = y.Max();
        if (Math.Abs(max - min) < 1e-12)
            return (1.0, min);

        double b = (max + min) / 2.0;
        double a = (max - min) / (2.0 * ScaleMax);
        return (a, b);
    }

    private static short Encode(double y, double a, double b)
    {
        if (Math.Abs(a) < 1e-18) return 0;
        var s = (y - b) / a;
        var r = (int)Math.Round(s);
        if (r > ScaleMax) r = ScaleMax;
        if (r < -ScaleMax) r = -ScaleMax;
        return (short)r;
    }

    private static string ResolveTimeCode(DateTimeOffset startUtc, string mode)
    {
        if (string.Equals(mode, "RUN_OFFSET", StringComparison.OrdinalIgnoreCase))
            return FormatOffset(startUtc.Offset);

        return "0h00";
    }

    private static string FormatOffset(TimeSpan off)
    {
        var sign = off < TimeSpan.Zero ? "-" : "";
        off = off.Duration();
        int h = off.Hours + off.Days * 24;
        int m = off.Minutes;
        return $"{sign}{h}h{m:00}";
    }

    private static string Fix1(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return "0";
        return s.Trim().Length >= 1 ? s.Trim()[0].ToString() : "0";
    }

    private static string MakeUnique(HashSet<string> used, string baseName)
    {
        var name = baseName;
        int k = 2;
        while (!used.Add(name))
            name = $"{baseName}_{k++}";
        return name;
    }

    private static string PadRightCsv(string s, int width)
    {
        // COMTRADE aceita espaços no ch_id; isso deixa o arquivo visualmente parecido com exemplos
        if (string.IsNullOrEmpty(s)) return s;
        return s.Length >= width ? s : s.PadRight(width);
    }
}