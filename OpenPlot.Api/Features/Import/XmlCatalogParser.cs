using System.Globalization;
using System.Xml.Linq;

namespace OpenPlot.Features.Import;

internal interface IXmlCatalogParser
{
    ParsedCatalogFile Parse(string path);
}

internal sealed class XmlCatalogParser : IXmlCatalogParser
{
    public ParsedCatalogFile Parse(string path)
    {
        var doc = XDocument.Load(path);
        var ns = doc.Root?.Name.Namespace ?? XNamespace.None;
        var notes = new List<string>();

        var pdc = ParsePdc(doc, ns, path, notes);
        var pmus = doc.Descendants(ns + "pmu")
            .Select(pmu => ParsePmu(pmu, ns))
            .ToList();

        if (pmus.Count == 0)
            notes.Add("Nenhuma <pmu> encontrada.");

        return new ParsedCatalogFile(path, pdc, pmus, notes);
    }

    private static ParsedPdc ParsePdc(XDocument doc, XNamespace ns, string path, List<string> notes)
    {
        var pdcElem = doc.Descendants(ns + "pdc").FirstOrDefault();
        if (pdcElem is null)
        {
            notes.Add("Arquivo sem <pdc>: criado PDC sintético a partir do nome do arquivo.");
            return new ParsedPdc(
                Path.GetFileNameWithoutExtension(path),
                "unknown",
                string.Empty,
                60,
                string.Empty,
                string.Empty,
                string.Empty);
        }

        var name = Value(pdcElem.Element(ns + "name")) ?? Path.GetFileNameWithoutExtension(path);
        var kind = Value(pdcElem.Element(ns + "type")) ?? "unknown";
        var address = Value(pdcElem.Element(ns + "address")) ?? string.Empty;
        var fps = ParseInt(Value(pdcElem.Element(ns + "fps")), 60);
        var security = pdcElem.Element(ns + "security");
        var user = Value(security?.Element(ns + "user")) ?? string.Empty;
        var password = Value(security?.Element(ns + "pswd")) ?? string.Empty;
        var databaseName = string.Equals(kind, "medfasee", StringComparison.OrdinalIgnoreCase)
            ? Value(pdcElem.Element(ns + "dataBank")) ?? string.Empty
            : string.Empty;

        return new ParsedPdc(name, kind, address, fps, user, password, databaseName);
    }

    private static ParsedPmu ParsePmu(XElement pmu, XNamespace ns)
    {
        var idName = Value(pmu.Element(ns + "idName")) ?? string.Empty;
        var fullName = Value(pmu.Element(ns + "fullName")) ?? idName;
        var voltLevel = ParseInt(Value(pmu.Element(ns + "voltLevel")), 0);
        var idNumberRaw = Value(pmu.Element(ns + "idNumber"));
        int? idNumber = int.TryParse(idNumberRaw, out var parsedIdNumber) && parsedIdNumber > 0
            ? parsedIdNumber
            : null;

        var local = pmu.Element(ns + "local");
        var area = Value(local?.Element(ns + "area")) ?? string.Empty;
        var state = Value(local?.Element(ns + "state")) ?? string.Empty;
        var station = Value(local?.Element(ns + "station")) ?? string.Empty;
        var latitude = ParseDouble(Value(local?.Element(ns + "lat")));
        var longitude = ParseDouble(Value(local?.Element(ns + "lon")));
        var signals = ParseSignals(idName, pmu.Element(ns + "measurements"), ns);

        return new ParsedPmu(idName, fullName, voltLevel, idNumber, area, state, station, latitude, longitude, signals);
    }

    private static IReadOnlyList<ParsedSignal> ParseSignals(string idName, XElement? measurements, XNamespace ns)
    {
        if (measurements is null)
            return Array.Empty<ParsedSignal>();

        var signals = new List<ParsedSignal>();

        foreach (var phasor in measurements.Elements(ns + "phasor"))
        {
            var name = Value(phasor.Element(ns + "pName")) ?? string.Empty;
            var quantity = Value(phasor.Element(ns + "pType")) ?? string.Empty;
            var phase = Value(phasor.Element(ns + "pPhase")) ?? string.Empty;
            var channelIdElement = phasor.Element(ns + "chId");

            if (channelIdElement is not null)
            {
                var channelId = ParseInt(Value(channelIdElement), 0);
                signals.Add(CreateSignal(name, quantity, phase, "MAG", channelId, $"Sinal ignorado (MAG/chId) sem historian_point (>0): {idName}:{name}"));
                signals.Add(CreateSignal(name, quantity, phase, "ANG", channelId, $"Sinal ignorado (ANG/chId) sem historian_point (>0): {idName}:{name}"));
                continue;
            }

            var modId = ParseInt(Value(phasor.Element(ns + "modId")), 0);
            var angId = ParseInt(Value(phasor.Element(ns + "angId")), 0);
            signals.Add(CreateSignal(name, quantity, phase, "MAG", modId, $"Sinal ignorado (MAG) sem historian_point (>0): {idName}:{name}"));
            signals.Add(CreateSignal(name, quantity, phase, "ANG", angId, $"Sinal ignorado (ANG) sem historian_point (>0): {idName}:{name}"));
        }

        var frequency = measurements.Element(ns + "freq");
        if (frequency is not null)
        {
            var name = Value(frequency.Element(ns + "fName")) ?? "FREQUENCIA";
            var historianPoint = ParseInt(Value(frequency.Element(ns + "fId")), 0);
            signals.Add(CreateSignal(name, "Frequency", "None", "FREQ", historianPoint, $"Sinal ignorado (FREQ) sem historian_point (>0): {idName}:{name}"));
        }

        var deltaFrequency = measurements.Element(ns + "dFreq");
        if (deltaFrequency is not null)
        {
            var name = Value(deltaFrequency.Element(ns + "dfName")) ?? "DFREQ";
            var historianPoint = ParseInt(Value(deltaFrequency.Element(ns + "dfId")), 0);
            signals.Add(CreateSignal(name, "Frequency", "None", "DFREQ", historianPoint, $"Sinal ignorado (DFREQ) sem historian_point (>0): {idName}:{name}"));
        }

        foreach (var analog in measurements.Elements(ns + "analog"))
        {
            var analogTypeRaw = Value(analog.Element(ns + "aType")) ?? string.Empty;
            var analogType = analogTypeRaw.Trim().ToUpperInvariant();
            if (analogType is not ("VTHD" or "CTHD" or "THD"))
                continue;

            var analogName = Value(analog.Element(ns + "aName")) ?? analogTypeRaw;
            var analogPhase = Value(analog.Element(ns + "aPhase")) ?? string.Empty;
            var analogId = ParseInt(Value(analog.Element(ns + "aId")), 0);
            var quantity = analogType.StartsWith("V")
                ? "Voltage"
                : analogType.StartsWith("C")
                    ? "Current"
                    : "Analog";

            signals.Add(CreateSignal(quantity: quantity, phase: analogPhase, component: "THD", historianPoint: analogId, name: analogName,
                notInsertedNote: analogId <= 0
                    ? $"Sinal THD ignorado sem historian_point (>0): {idName}:{analogName}"
                    : $"Falha ao inserir THD: {idName}:{analogName} (aId={analogId})"));
        }

        foreach (var digital in measurements.Elements(ns + "digital"))
        {
            var digitalTypeRaw = Value(digital.Element(ns + "dType")) ?? string.Empty;
            var digitalName = Value(digital.Element(ns + "dName")) ?? digitalTypeRaw;
            var digitalId = ParseInt(Value(digital.Element(ns + "dId")), 0);
            signals.Add(CreateSignal(digitalName, "Digital", "None", "DIG", digitalId,
                digitalId <= 0
                    ? $"Sinal DIGITAL ignorado sem historian_point (>0): {idName}:{digitalName}"
                    : $"Falha ao inserir DIGITAL: {idName}:{digitalName} (dId={digitalId})"));
        }

        return signals;
    }

    private static ParsedSignal CreateSignal(string name, string quantity, string phase, string component, int historianPoint, string notInsertedNote)
        => new(name, quantity, phase, component, historianPoint, notInsertedNote);

    private static string? Value(XElement? element) => element?.Value?.Trim();

    private static int ParseInt(string? value, int defaultValue = 0)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : defaultValue;

    private static double? ParseDouble(string? value)
        => double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var number)
            ? number
            : null;
}
