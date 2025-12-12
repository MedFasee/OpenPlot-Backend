using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Npgsql;

namespace OpenPlot.XmlImporter
{
    public sealed class XmlImporter
    {
        private readonly string _connectionString;

        public XmlImporter(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        // --------- DTO de retorno (para logs/Swagger) ----------
        public sealed class ImportSummary
        {
            public string File { get; set; } = "";
            public int PdcId { get; set; }
            public int Pmus { get; set; }
            public int Signals { get; set; }
            public List<string> Notes { get; } = new();
        }

        // =============== API principal =================
        public async Task<List<ImportSummary>> RunAsync(string xmlPathOrFolder, CancellationToken ct = default)
        {
            var summaries = new List<ImportSummary>();

            var files = ResolveFiles(xmlPathOrFolder);
            if (files.Length == 0) return summaries;

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            foreach (var path in files)
            {
                var sum = new ImportSummary { File = path };
                try
                {
                    var doc = XDocument.Load(path);
                    var ns = doc.Root?.Name.Namespace ?? XNamespace.None;

                    // ---------- PDC ----------
                    var pdcElem = doc.Descendants(ns + "pdc").FirstOrDefault();
                    string pdcName, pdcKind, pdcAddr;
                    int pdcFps;
                    string dbName = "";

                    string user = "", pswd = "";
                    if (pdcElem is not null)
                    {
                        pdcName = Value(pdcElem.Element(ns + "name")) ?? Path.GetFileNameWithoutExtension(path);
                        pdcKind = Value(pdcElem.Element(ns + "type")) ?? "unknown";
                        pdcAddr = Value(pdcElem.Element(ns + "address")) ?? "";
                        pdcFps = ParseInt(Value(pdcElem.Element(ns + "fps")), 60);

                        var sec = pdcElem.Element(ns + "security");
                        user = Value(sec?.Element(ns + "user")) ?? "";
                        pswd = Value(sec?.Element(ns + "pswd")) ?? "";

                        // MedFasee tem <dataBank>
                        if (string.Equals(pdcKind, "medfasee", StringComparison.OrdinalIgnoreCase))
                        {
                            dbName = Value(pdcElem.Element(ns + "dataBank")) ?? "";
                        }

                        sum.PdcId = await UpsertPdc(conn, pdcName, pdcKind, pdcFps, pdcAddr, user, pswd, dbName, ct);
                    }
                    else
                    {
                        pdcName = Path.GetFileNameWithoutExtension(path);
                        pdcKind = "unknown";
                        pdcAddr = "";
                        pdcFps = 60;
                        dbName = "";

                        sum.Notes.Add("Arquivo sem <pdc>: criado PDC sintético a partir do nome do arquivo.");
                        sum.PdcId = await UpsertPdc(conn, pdcName, pdcKind, pdcFps, pdcAddr, "", "", dbName, ct);
                    }

                    // ---------- PMUs ----------
                    var pmuNodes = doc.Descendants(ns + "pmu").ToList();
                    if (pmuNodes.Count == 0)
                    {
                        sum.Notes.Add("Nenhuma <pmu> encontrada.");
                        summaries.Add(sum);
                        continue;
                    }

                    foreach (var pmu in pmuNodes)
                    {
                        var idName = Value(pmu.Element(ns + "idName")) ?? "";
                        var fullName = Value(pmu.Element(ns + "fullName")) ?? idName;
                        var voltLvl = ParseInt(Value(pmu.Element(ns + "voltLevel")), 0);

                        var idNumberRaw = Value(pmu.Element(ns + "idNumber"));
                        int? idNumber = null;
                        if (int.TryParse(idNumberRaw, out var n) && n > 0)
                            idNumber = n;

                        var local = pmu.Element(ns + "local");
                        var area = Value(local?.Element(ns + "area")) ?? "";
                        var state = Value(local?.Element(ns + "state")) ?? "";
                        var station = Value(local?.Element(ns + "station")) ?? "";
                        var lat = ParseDouble(Value(local?.Element(ns + "lat")));
                        var lon = ParseDouble(Value(local?.Element(ns + "lon")));

                        // PMU canônica (id_name único)
                        var pmuId = await UpsertPmu(conn, idName, fullName, voltLvl, area, state, station, lat, lon, ct);
                        sum.Pmus++;

                        // Associação PDC×PMU (pdc_local_id = idName no PDC)
                        var pdcPmuId = await UpsertPdcPmu(conn, sum.PdcId, pmuId, idName, idNumber, ct);

                        // ---- sinais ----
                        var meas = pmu.Element(ns + "measurements");
                        if (meas is null) continue;

                        // --------- PHASORS (MAG/ANG) ----------
                        foreach (var ph in meas.Elements(ns + "phasor"))
                        {
                            var pName = Value(ph.Element(ns + "pName")) ?? "";
                            var pType = Value(ph.Element(ns + "pType")) ?? "";   // Voltage | Current
                            var pPhase = Value(ph.Element(ns + "pPhase")) ?? "";   // A | B | C

                            var chIdElem = ph.Element(ns + "chId");
                            if (chIdElem != null)
                            {
                                // MedFasee: um único chId
                                var chId = ParseInt(Value(chIdElem), 0);

                                // MAG
                                var insMag = await UpsertSignal(
                                    conn,
                                    pdcPmuId,
                                    name: pName,
                                    quantity: pType,
                                    phase: pPhase,
                                    component: "MAG",
                                    historianPoint: chId,
                                    ct: ct);
                                sum.Signals += insMag;
                                if (chId <= 0 && insMag == 0)
                                    sum.Notes.Add($"Sinal ignorado (MAG/chId) sem historian_point (>0): {idName}:{pName}");

                                // ANG
                                var insAng = await UpsertSignal(
                                    conn,
                                    pdcPmuId,
                                    name: pName,
                                    quantity: pType,
                                    phase: pPhase,
                                    component: "ANG",
                                    historianPoint: chId,
                                    ct: ct);
                                sum.Signals += insAng;
                                if (chId <= 0 && insAng == 0)
                                    sum.Notes.Add($"Sinal ignorado (ANG/chId) sem historian_point (>0): {idName}:{pName}");
                            }
                            else
                            {
                                // Historian: modId + angId separados
                                var modId = ParseInt(Value(ph.Element(ns + "modId")), 0);
                                var angId = ParseInt(Value(ph.Element(ns + "angId")), 0);

                                // MAG
                                var ins1 = await UpsertSignal(
                                    conn,
                                    pdcPmuId,
                                    name: pName,
                                    quantity: pType,
                                    phase: pPhase,
                                    component: "MAG",
                                    historianPoint: modId,
                                    ct: ct);
                                sum.Signals += ins1;
                                if (modId <= 0 && ins1 == 0)
                                    sum.Notes.Add($"Sinal ignorado (MAG) sem historian_point (>0): {idName}:{pName}");

                                // ANG
                                var ins2 = await UpsertSignal(
                                    conn,
                                    pdcPmuId,
                                    name: pName,
                                    quantity: pType,
                                    phase: pPhase,
                                    component: "ANG",
                                    historianPoint: angId,
                                    ct: ct);
                                sum.Signals += ins2;
                                if (angId <= 0 && ins2 == 0)
                                    sum.Notes.Add($"Sinal ignorado (ANG) sem historian_point (>0): {idName}:{pName}");
                            }
                        }

                        // --------- FREQUENCY ----------
                        var f = meas.Element(ns + "freq");
                        if (f is not null)
                        {
                            var fName = Value(f.Element(ns + "fName")) ?? "FREQUENCIA";
                            var fId = ParseInt(Value(f.Element(ns + "fId")), 0);

                            var ins = await UpsertSignal(
                                conn, pdcPmuId,
                                fName, "Frequency", "None", "FREQ", fId, ct);
                            sum.Signals += ins;
                            if (fId <= 0 && ins == 0)
                                sum.Notes.Add($"Sinal ignorado (FREQ) sem historian_point (>0): {idName}:{fName}");
                        }

                        // --------- DFREQ ----------
                        var df = meas.Element(ns + "dFreq");
                        if (df is not null)
                        {
                            var dName = Value(df.Element(ns + "dfName")) ?? "DFREQ";
                            var dId = ParseInt(Value(df.Element(ns + "dfId")), 0);

                            var ins = await UpsertSignal(
                                conn, pdcPmuId,
                                dName, "Frequency", "None", "DFREQ", dId, ct);
                            sum.Signals += ins;
                            if (dId <= 0 && ins == 0)
                                sum.Notes.Add($"Sinal ignorado (DFREQ) sem historian_point (>0): {idName}:{dName}");
                        }

                        // --------- ANALOG (THD: VTHD / CTHD) ----------
                        foreach (var an in meas.Elements(ns + "analog"))
                        {
                            var aTypeRaw = Value(an.Element(ns + "aType")) ?? "";
                            var aType = aTypeRaw.Trim().ToUpperInvariant();

                            // Só nos interessam VTHD / CTHD / THD
                            if (aType is not ("VTHD" or "CTHD" or "THD"))
                                continue;

                            var aName = Value(an.Element(ns + "aName")) ?? aTypeRaw;
                            var aPhase = Value(an.Element(ns + "aPhase")) ?? "";

                            // Historian point: aId (no XML da foto)
                            var aId = ParseInt(Value(an.Element(ns + "aId")), 0);
                            if (aId <= 0)
                            {
                                sum.Notes.Add($"Sinal THD ignorado sem historian_point (>0): {idName}:{aName}");
                                continue;
                            }

                            // quantity: tensão ou corrente de acordo com o tipo
                            string qty =
                                aType.StartsWith("V") ? "Voltage" :
                                aType.StartsWith("C") ? "Current" :
                                "Analog";

                            var insThd = await UpsertSignal(
                                conn,
                                pdcPmuId,
                                name: aName,
                                quantity: qty,
                                phase: aPhase,
                                component: "THD",   // componente único na enum
                                historianPoint: aId,
                                ct: ct);

                            sum.Signals += insThd;
                            if (insThd == 0)
                                sum.Notes.Add($"Falha ao inserir THD: {idName}:{aName} (aId={aId})");
                        }

                        // --------- DIGITAL ----------
                        foreach (var dg in meas.Elements(ns + "digital"))
                        {
                            var dTypeRaw = Value(dg.Element(ns + "dType")) ?? "";
                            var dType = dTypeRaw.Trim().ToUpperInvariant();

                            var dName = Value(dg.Element(ns + "dName")) ?? dTypeRaw;
                            var dId = ParseInt(Value(dg.Element(ns + "dId")), 0);

                            if (dId <= 0)
                            {
                                sum.Notes.Add($"Sinal DIGITAL ignorado sem historian_point (>0): {idName}:{dName}");
                                continue;
                            }

                            // Sugestão de mapeamento:
                            // - phase: None
                            // - quantity: Digital
                            // - component: DIG (ou DIGITAL)
                            var ins = await UpsertSignal(
                                conn,
                                pdcPmuId,
                                name: dName,
                                quantity: "Digital",
                                phase: "None",
                                component: "DIG",
                                historianPoint: dId,
                                ct: ct);

                            sum.Signals += ins;
                            if (ins == 0)
                                sum.Notes.Add($"Falha ao inserir DIGITAL: {idName}:{dName} (dId={dId})");
                        }
                    }
                }
                catch (Exception ex)
                {
                    sum.Notes.Add("Erro: " + ex.Message);
                }

                summaries.Add(sum);
            }

            return summaries;
        }

        // =============== Helpers de XML / Parse ===============
        private static string? Value(XElement? e) => e?.Value?.Trim();

        private static int ParseInt(string? s, int def = 0)
            => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : def;

        private static double? ParseDouble(string? s)
            => double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var d) ? d : null;

        private static string[] ResolveFiles(string pathOrFolder)
        {
            if (File.Exists(pathOrFolder))
                return new[] { pathOrFolder };
            if (Directory.Exists(pathOrFolder))
                return Directory.GetFiles(pathOrFolder, "*.xml", SearchOption.TopDirectoryOnly);
            return Array.Empty<string>();
        }

        private static string NormalizeQty(string? s) =>
        s?.Trim() switch
        {
            "Voltage" or "voltage" or "VOLTAGE" or "V" or "Volt" => "Voltage",
            "Current" or "current" or "CURRENT" or "I" => "Current",
            "Frequency" or "frequency" or "FREQUENCY" => "Frequency",
            "Digital" or "digital" or "DIGITAL" or "D" => "Digital",
            _ => "Voltage"
        };

        private static string NormalizePhase(string? s)
        {
            var t = s?.Trim().ToUpperInvariant();
            return (t is "A" or "B" or "C") ? t! : "None";
        }

        // MAG / ANG / FREQ / DFREQ / THD (VTHD / CTHD mapeiam para THD)
        private static string NormalizeComp(string? s)
        {
            var t = s?.Trim().ToUpperInvariant();
            return t switch
            {
                "MAG" => "MAG",
                "ANG" => "ANG",
                "FREQ" => "FREQ",
                "DFREQ" => "DFREQ",
                "THD" or "VTHD" or "CTHD" => "THD",
                "DIG" or "DIGITAL" => "DIG",
                _ => "MAG"
            };
        }

        // =============== UPSERTS ===============
        private static async Task<int> UpsertPdc(
            NpgsqlConnection conn,
            string name, string kind, int fps, string addr,
            string userName, string password, string dbName,
            CancellationToken ct)
        {
            const string sql = @"
INSERT INTO openplot.pdc (name, kind, fps, address, user_name, password, db_name)
VALUES (@name, @kind, @fps, @addr, @user_name, @password, @db_name)
ON CONFLICT (name) DO UPDATE
SET kind      = EXCLUDED.kind,
    fps       = EXCLUDED.fps,
    address   = EXCLUDED.address,
    user_name = EXCLUDED.user_name,
    password  = EXCLUDED.password,
    db_name   = EXCLUDED.db_name
RETURNING pdc_id;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("name", name);
            cmd.Parameters.AddWithValue("kind", kind);
            cmd.Parameters.AddWithValue("fps", fps);
            cmd.Parameters.AddWithValue("addr", addr ?? "");
            cmd.Parameters.AddWithValue("user_name", (object?)userName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("password", (object?)password ?? DBNull.Value);
            cmd.Parameters.AddWithValue("db_name", (object?)dbName ?? DBNull.Value);

            var id = await cmd.ExecuteScalarAsync(ct);
            return Convert.ToInt32(id, CultureInfo.InvariantCulture);
        }

        private static async Task<int> UpsertPmu(
            NpgsqlConnection conn,
            string idName, string fullName, int voltLevel,
            string area, string state, string station, double? lat, double? lon,
            CancellationToken ct)
        {
            const string sql = @"
INSERT INTO openplot.pmu (id_name, full_name, volt_level, area, state, station, lat, lon)
VALUES (@id_name, @full_name, @volt_level, @area, @state, @station, @lat, @lon)
ON CONFLICT (id_name) DO UPDATE
SET full_name  = EXCLUDED.full_name,
    volt_level = EXCLUDED.volt_level,
    area       = EXCLUDED.area,
    state      = EXCLUDED.state,
    station    = EXCLUDED.station,
    lat        = EXCLUDED.lat,
    lon        = EXCLUDED.lon
RETURNING pmu_id;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id_name", idName);
            cmd.Parameters.AddWithValue("full_name", (object?)fullName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("volt_level", voltLevel);
            cmd.Parameters.AddWithValue("area", (object?)area ?? DBNull.Value);
            cmd.Parameters.AddWithValue("state", (object?)state ?? DBNull.Value);
            cmd.Parameters.AddWithValue("station", (object?)station ?? DBNull.Value);
            cmd.Parameters.AddWithValue("lat", (object?)lat ?? DBNull.Value);
            cmd.Parameters.AddWithValue("lon", (object?)lon ?? DBNull.Value);

            var id = await cmd.ExecuteScalarAsync(ct);
            return Convert.ToInt32(id, CultureInfo.InvariantCulture);
        }

        private static async Task<int> UpsertPdcPmu(
            NpgsqlConnection conn,
            int pdcId, int pmuId, string pdcLocalId,
            int? localNumericId,
            CancellationToken ct)
        {
            const string sql = @"
INSERT INTO openplot.pdc_pmu (pdc_id, pmu_id, pdc_local_id, local_numeric_id)
VALUES (@pdc_id, @pmu_id, @pdc_local_id, @local_numeric_id)
ON CONFLICT (pdc_id, pmu_id) DO UPDATE
SET pdc_local_id     = EXCLUDED.pdc_local_id,
    local_numeric_id = EXCLUDED.local_numeric_id
RETURNING pdc_pmu_id;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("pdc_id", pdcId);
            cmd.Parameters.AddWithValue("pmu_id", pmuId);
            cmd.Parameters.AddWithValue("pdc_local_id", (object?)pdcLocalId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("local_numeric_id", (object?)localNumericId ?? DBNull.Value);

            var id = await cmd.ExecuteScalarAsync(ct);
            return Convert.ToInt32(id, CultureInfo.InvariantCulture);
        }

        private static async Task<int> UpsertSignal(
            NpgsqlConnection conn,
            int pdcPmuId,
            string name,
            string quantity,
            string phase,
            string component,
            int historianPoint,
            CancellationToken ct)
        {
            if (historianPoint < 0) return 0;

            var qty = NormalizeQty(quantity);
            var ph = NormalizePhase(phase);
            var comp = NormalizeComp(component);

            const string sql = @"
INSERT INTO openplot.signal (pdc_pmu_id, name, quantity, phase, component, historian_point)
VALUES (@pdc_pmu_id, @name, @quantity::qty_kind, @phase::phase_kind, @component::comp_kind, @historian_point)
ON CONFLICT (pdc_pmu_id, name, phase, component) DO UPDATE
SET quantity        = EXCLUDED.quantity,
    historian_point = EXCLUDED.historian_point
RETURNING signal_id;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("pdc_pmu_id", pdcPmuId);
            cmd.Parameters.AddWithValue("name", name);
            cmd.Parameters.AddWithValue("quantity", qty);
            cmd.Parameters.AddWithValue("phase", ph);
            cmd.Parameters.AddWithValue("component", comp);
            cmd.Parameters.AddWithValue("historian_point", historianPoint);

            var idObj = await cmd.ExecuteScalarAsync(ct);
            return idObj != null ? 1 : 0;
        }
    }
}