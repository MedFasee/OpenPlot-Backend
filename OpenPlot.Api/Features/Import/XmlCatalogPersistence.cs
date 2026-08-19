using System.Globalization;
using Npgsql;

namespace OpenPlot.Features.Import;

internal interface IXmlCatalogPersistence
{
    Task<XmlCatalogImporter.ImportSummary> PersistAsync(
        ParsedCatalogFile file,
        NpgsqlConnection conn,
        CancellationToken ct);
}

internal sealed class XmlCatalogPersistence : IXmlCatalogPersistence
{
    public async Task<XmlCatalogImporter.ImportSummary> PersistAsync(
        ParsedCatalogFile file,
        NpgsqlConnection conn,
        CancellationToken ct)
    {
        var summary = new XmlCatalogImporter.ImportSummary
        {
            File = file.FilePath
        };

        summary.Notes.AddRange(file.Notes);

        summary.PdcId = await UpsertPdc(
            conn,
            file.Pdc.Name,
            file.Pdc.Kind,
            file.Pdc.Fps,
            file.Pdc.Address,
            file.Pdc.UserName,
            file.Pdc.Password,
            file.Pdc.DatabaseName,
            ct);

        if (file.Pmus.Count == 0)
            return summary;

        foreach (var pmu in file.Pmus)
        {
            var pmuId = await UpsertPmu(
                conn,
                pmu.IdName,
                pmu.FullName,
                pmu.VoltLevel,
                pmu.Area,
                pmu.State,
                pmu.Station,
                pmu.Latitude,
                pmu.Longitude,
                ct);

            summary.Pmus++;

            var pdcPmuId = await UpsertPdcPmu(
                conn,
                summary.PdcId,
                pmuId,
                pmu.IdName,
                pmu.IdNumber,
                ct);

            foreach (var signal in pmu.Signals)
            {
                var inserted = await UpsertSignal(
                    conn,
                    pdcPmuId,
                    signal.Name,
                    signal.Quantity,
                    signal.Phase,
                    signal.Component,
                    signal.HistorianPoint,
                    ct);

                summary.Signals += inserted;

                if (inserted == 0 &&
                    !string.IsNullOrWhiteSpace(signal.NotInsertedNote))
                {
                    summary.Notes.Add(signal.NotInsertedNote);
                }
            }

            /*
             * Calcula o tipo da associação PDC/PMU com base nos sinais
             * efetivamente persistidos.
             *
             * IMPORTANTE:
             * - somente preenche quando kind está NULL ou vazio;
             * - nunca sobrescreve classificações preexistentes.
             *
             * Ordem da assinatura:
             *
             * F = Frequency
             * V = Voltage fasorial (MAG/ANG)
             * I = Current fasorial (MAG/ANG)
             * D = Digital
             * H = THD
             *
             * Exemplos:
             * FV
             * FI
             * FVI
             * FID
             * FVIH
             */
            await FillPdcPmuKindIfEmpty(
                conn,
                pdcPmuId,
                ct);
        }

        return summary;
    }

    private static string NormalizeQty(string? value) =>
        value?.Trim() switch
        {
            "Voltage" or "voltage" or "VOLTAGE" or "V" or "Volt"
                => "Voltage",

            "Current" or "current" or "CURRENT" or "I"
                => "Current",

            "Frequency" or "frequency" or "FREQUENCY"
                => "Frequency",

            "Digital" or "digital" or "DIGITAL" or "D"
                => "Digital",

            _ => "Voltage"
        };

    private static string NormalizePhase(string? value)
    {
        var normalized = value?.Trim().ToUpperInvariant();

        return normalized is "A" or "B" or "C"
            ? normalized
            : "None";
    }

    private static string NormalizeComp(string? value)
    {
        var normalized = value?.Trim().ToUpperInvariant();

        return normalized switch
        {
            "MAG" => "MAG",
            "ANG" => "ANG",

            "FREQ" => "FREQ",
            "DFREQ" => "DFREQ",

            "THD" or "VTHD" or "CTHD"
                => "THD",

            "DIG" or "DIGITAL"
                => "DIG",

            _ => "MAG"
        };
    }

    private static async Task<int> UpsertPdc(
        NpgsqlConnection conn,
        string name,
        string kind,
        int fps,
        string addr,
        string userName,
        string password,
        string dbName,
        CancellationToken ct)
    {
        const string sql = @"
INSERT INTO openplot.pdc
(
    name,
    kind,
    fps,
    address,
    user_name,
    password,
    db_name
)
VALUES
(
    @name,
    @kind,
    @fps,
    @addr,
    @user_name,
    @password,
    @db_name
)
ON CONFLICT (name) DO UPDATE
SET
    kind      = EXCLUDED.kind,
    fps       = EXCLUDED.fps,
    address   = EXCLUDED.address,
    user_name = EXCLUDED.user_name,
    password  = EXCLUDED.password,
    db_name   = EXCLUDED.db_name
RETURNING pdc_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);

        cmd.Parameters.AddWithValue(
            "name",
            name);

        cmd.Parameters.AddWithValue(
            "kind",
            kind);

        cmd.Parameters.AddWithValue(
            "fps",
            fps);

        cmd.Parameters.AddWithValue(
            "addr",
            addr ?? string.Empty);

        cmd.Parameters.AddWithValue(
            "user_name",
            (object?)userName ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "password",
            (object?)password ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "db_name",
            (object?)dbName ?? DBNull.Value);

        var id = await cmd.ExecuteScalarAsync(ct);

        return Convert.ToInt32(
            id,
            CultureInfo.InvariantCulture);
    }

    private static async Task<int> UpsertPmu(
        NpgsqlConnection conn,
        string idName,
        string fullName,
        int voltLevel,
        string area,
        string state,
        string station,
        double? lat,
        double? lon,
        CancellationToken ct)
    {
        const string sql = @"
INSERT INTO openplot.pmu
(
    id_name,
    full_name,
    volt_level,
    area,
    state,
    station,
    lat,
    lon
)
VALUES
(
    @id_name,
    @full_name,
    @volt_level,
    @area,
    @state,
    @station,
    @lat,
    @lon
)
ON CONFLICT (id_name) DO UPDATE
SET
    full_name  = EXCLUDED.full_name,
    volt_level = EXCLUDED.volt_level,
    area       = EXCLUDED.area,
    state      = EXCLUDED.state,
    station    = EXCLUDED.station,
    lat        = EXCLUDED.lat,
    lon        = EXCLUDED.lon
RETURNING pmu_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);

        cmd.Parameters.AddWithValue(
            "id_name",
            idName);

        cmd.Parameters.AddWithValue(
            "full_name",
            (object?)fullName ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "volt_level",
            voltLevel);

        cmd.Parameters.AddWithValue(
            "area",
            (object?)area ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "state",
            (object?)state ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "station",
            (object?)station ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "lat",
            (object?)lat ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "lon",
            (object?)lon ?? DBNull.Value);

        var id = await cmd.ExecuteScalarAsync(ct);

        return Convert.ToInt32(
            id,
            CultureInfo.InvariantCulture);
    }

    private static async Task<int> UpsertPdcPmu(
        NpgsqlConnection conn,
        int pdcId,
        int pmuId,
        string pdcLocalId,
        int? localNumericId,
        CancellationToken ct)
    {
        /*
         * Não calculamos o kind aqui.
         *
         * Neste momento os sinais da PMU ainda não foram persistidos.
         * O kind será calculado após todos os UpsertSignal daquela PMU.
         *
         * Também não incluímos kind no UPDATE para evitar sobrescrever
         * qualquer classificação já existente.
         */
        const string sql = @"
INSERT INTO openplot.pdc_pmu
(
    pdc_id,
    pmu_id,
    pdc_local_id,
    local_numeric_id
)
VALUES
(
    @pdc_id,
    @pmu_id,
    @pdc_local_id,
    @local_numeric_id
)
ON CONFLICT (pdc_id, pmu_id) DO UPDATE
SET
    pdc_local_id     = EXCLUDED.pdc_local_id,
    local_numeric_id = EXCLUDED.local_numeric_id
RETURNING pdc_pmu_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);

        cmd.Parameters.AddWithValue(
            "pdc_id",
            pdcId);

        cmd.Parameters.AddWithValue(
            "pmu_id",
            pmuId);

        cmd.Parameters.AddWithValue(
            "pdc_local_id",
            (object?)pdcLocalId ?? DBNull.Value);

        cmd.Parameters.AddWithValue(
            "local_numeric_id",
            (object?)localNumericId ?? DBNull.Value);

        var id = await cmd.ExecuteScalarAsync(ct);

        return Convert.ToInt32(
            id,
            CultureInfo.InvariantCulture);
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
        if (historianPoint < 0)
            return 0;

        var qty = NormalizeQty(quantity);
        var normalizedPhase = NormalizePhase(phase);
        var normalizedComponent = NormalizeComp(component);

        const string sql = @"
INSERT INTO openplot.signal
(
    pdc_pmu_id,
    name,
    quantity,
    phase,
    component,
    historian_point
)
VALUES
(
    @pdc_pmu_id,
    @name,
    @quantity::qty_kind,
    @phase::phase_kind,
    @component::comp_kind,
    @historian_point
)
ON CONFLICT
(
    pdc_pmu_id,
    name,
    phase,
    component
)
DO UPDATE
SET
    quantity        = EXCLUDED.quantity,
    historian_point = EXCLUDED.historian_point
RETURNING signal_id;";

        await using var cmd = new NpgsqlCommand(sql, conn);

        cmd.Parameters.AddWithValue(
            "pdc_pmu_id",
            pdcPmuId);

        cmd.Parameters.AddWithValue(
            "name",
            name);

        cmd.Parameters.AddWithValue(
            "quantity",
            qty);

        cmd.Parameters.AddWithValue(
            "phase",
            normalizedPhase);

        cmd.Parameters.AddWithValue(
            "component",
            normalizedComponent);

        cmd.Parameters.AddWithValue(
            "historian_point",
            historianPoint);

        var id = await cmd.ExecuteScalarAsync(ct);

        return id != null
            ? 1
            : 0;
    }

    private static async Task FillPdcPmuKindIfEmpty(
        NpgsqlConnection conn,
        int pdcPmuId,
        CancellationToken ct)
    {
        /*
         * Assinatura do kind:
         *
         * F = existe Frequency
         *
         * V = existe Voltage fasorial
         *     component MAG ou ANG
         *
         * I = existe Current fasorial
         *     component MAG ou ANG
         *
         * D = existe Digital/DIG
         *
         * H = existe THD
         *
         * A ordem é sempre:
         *
         * F -> V -> I -> D -> H
         *
         * IMPORTANTE:
         * O UPDATE somente ocorre se o kind atual estiver
         * NULL ou vazio.
         */
        const string sql = @"
WITH signal_kind AS
(
    SELECT
        s.pdc_pmu_id,

        BOOL_OR(
            s.quantity::text = 'Frequency'
        ) AS has_frequency,

        BOOL_OR(
            s.quantity::text = 'Voltage'
            AND s.component::text IN ('MAG', 'ANG')
        ) AS has_voltage,

        BOOL_OR(
            s.quantity::text = 'Current'
            AND s.component::text IN ('MAG', 'ANG')
        ) AS has_current,

        BOOL_OR(
            s.quantity::text = 'Digital'
            OR s.component::text = 'DIG'
        ) AS has_digital,

        BOOL_OR(
            s.component::text = 'THD'
        ) AS has_thd

    FROM openplot.signal s

    WHERE s.pdc_pmu_id = @pdc_pmu_id

    GROUP BY
        s.pdc_pmu_id
),
calculated AS
(
    SELECT
        pdc_pmu_id,

        CONCAT(
            CASE
                WHEN has_frequency THEN 'F'
                ELSE ''
            END,

            CASE
                WHEN has_voltage THEN 'V'
                ELSE ''
            END,

            CASE
                WHEN has_current THEN 'I'
                ELSE ''
            END,

            CASE
                WHEN has_digital THEN 'D'
                ELSE ''
            END,

            CASE
                WHEN has_thd THEN 'H'
                ELSE ''
            END
        ) AS kind

    FROM signal_kind
)
UPDATE openplot.pdc_pmu pp

SET kind = calculated.kind

FROM calculated

WHERE pp.pdc_pmu_id = calculated.pdc_pmu_id

  AND pp.pdc_pmu_id = @pdc_pmu_id

  AND (
        pp.kind IS NULL
        OR BTRIM(pp.kind) = ''
  )

  AND calculated.kind <> '';";

        await using var cmd = new NpgsqlCommand(sql, conn);

        cmd.Parameters.AddWithValue(
            "pdc_pmu_id",
            pdcPmuId);

        await cmd.ExecuteNonQueryAsync(ct);
    }
}