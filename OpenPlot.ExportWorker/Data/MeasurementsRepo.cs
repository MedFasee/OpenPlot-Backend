using Dapper;
using OpenPlot.ExportWorker.Domain;
using OpenPlot.ExportWorker.Options;

namespace OpenPlot.ExportWorker.Data;

public sealed class MeasurementsRepo
{
    private readonly Db _db;
    private readonly int _queryTimeoutSeconds;

    public MeasurementsRepo(
        Db db,
        Microsoft.Extensions.Options.IOptions<ExportOptions> options)
    {
        _db = db;
        _queryTimeoutSeconds =
            Math.Max(30, options.Value.MeasurementsQueryTimeoutSeconds);
    }

    /// <summary>
    /// Resolve a seleção de PMUs a partir do run_id,
    /// encontra os sinais associados a cada PDC/PMU
    /// e busca os valores na measurements.
    ///
    /// A fase dos canais é obtida exclusivamente de openplot.signal.phase.
    ///
    /// Para Voltage / Current / THD:
    /// A -> Am
    /// B -> Bm
    /// C -> Cm
    ///
    /// Frequency / DFREQ / Digital não possuem fase no CFG.
    /// </summary>
    public async Task<List<MeasurementRow>> LoadMeasurementsForComtradeAsync(
        Guid runId,
        DateTimeOffset? fromUtc,
        DateTimeOffset? toUtc,
        string[]? pmusOverride,
        CancellationToken ct)
    {
        var valueCase = @"
CASE
    -- ============================================================
    -- TENSÃO
    -- ============================================================

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'A'
         AND s.component = 'MAG'
        THEN mw.va_mod_v

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'A'
         AND s.component = 'ANG'
        THEN mw.va_ang_deg

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'B'
         AND s.component = 'MAG'
        THEN mw.vb_mod_v

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'B'
         AND s.component = 'ANG'
        THEN mw.vb_ang_deg

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'C'
         AND s.component = 'MAG'
        THEN mw.vc_mod_v

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'C'
         AND s.component = 'ANG'
        THEN mw.vc_ang_deg


    -- ============================================================
    -- CORRENTE
    -- ============================================================

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'A'
         AND s.component = 'MAG'
        THEN mw.ia_mod_a

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'A'
         AND s.component = 'ANG'
        THEN mw.ia_ang_deg

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'B'
         AND s.component = 'MAG'
        THEN mw.ib_mod_a

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'B'
         AND s.component = 'ANG'
        THEN mw.ib_ang_deg

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'C'
         AND s.component = 'MAG'
        THEN mw.ic_mod_a

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'C'
         AND s.component = 'ANG'
        THEN mw.ic_ang_deg


    -- ============================================================
    -- THD DE CORRENTE
    -- ============================================================

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'A'
         AND s.component = 'THD'
        THEN mw.cthd_a_pct

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'B'
         AND s.component = 'THD'
        THEN mw.cthd_b_pct

    WHEN LOWER(s.quantity) IN ('current', 'i')
         AND s.phase_raw = 'C'
         AND s.component = 'THD'
        THEN mw.cthd_c_pct


    -- ============================================================
    -- THD DE TENSÃO
    -- ============================================================

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'A'
         AND s.component = 'THD'
        THEN mw.vthd_a_pct

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'B'
         AND s.component = 'THD'
        THEN mw.vthd_b_pct

    WHEN LOWER(s.quantity) IN ('voltage', 'v')
         AND s.phase_raw = 'C'
         AND s.component = 'THD'
        THEN mw.vthd_c_pct


    -- ============================================================
    -- FREQUÊNCIA
    -- ============================================================

    WHEN LOWER(s.quantity) IN ('frequency', 'freq')
         AND LOWER(s.component) = 'freq'
        THEN mw.frequency_hz

    WHEN LOWER(s.quantity) IN ('frequency', 'freq')
         AND LOWER(s.component) = 'dfreq'
        THEN mw.delta_freq_hz


    -- ============================================================
    -- DIGITAL
    -- ============================================================

    WHEN LOWER(s.quantity) IN ('digital', 'd')
         AND s.component = 'DIG'
         AND UPPER(COALESCE(s.signal_name, '')) = 'cfds_dig'
        THEN mw.cfds_dig

    ELSE NULL
END";

        var sql = @"
WITH run AS
(
    SELECT
        id,
        source AS pdc_name,
        from_ts,
        to_ts,
        COALESCE(pmus_ok, pmus) AS pmus,
        signals

    FROM openplot.search_runs

    WHERE id = @run_id::uuid
),

run_window AS
(
    SELECT
        CASE
            WHEN pg_typeof(r.from_ts)::text =
                 'timestamp without time zone'
                THEN r.from_ts::timestamptz
            ELSE r.from_ts
        END AS from_utc,

        CASE
            WHEN pg_typeof(r.to_ts)::text =
                 'timestamp without time zone'
                THEN r.to_ts::timestamptz
            ELSE r.to_ts
        END AS to_utc,

        r.pdc_name,
        r.signals,
        r.pmus

    FROM run r
),

win AS
(
    SELECT
        COALESCE(@from_utc, rw.from_utc) AS from_utc,
        COALESCE(@to_utc,   rw.to_utc)   AS to_utc,

        rw.pdc_name,
        rw.signals,
        rw.pmus

    FROM run_window rw
),

src AS
(
    SELECT
        w.pdc_name,
        w.from_utc AS from_ts,
        w.to_utc   AS to_ts,

        CASE
            WHEN @pmus IS NOT NULL
                THEN to_jsonb(@pmus::text[])

            WHEN jsonb_typeof(w.pmus) = 'array'
                 AND jsonb_array_length(w.pmus) > 0
                THEN w.pmus

            ELSE '[]'::jsonb
        END AS arr

    FROM win w
),

elems AS
(
    SELECT
        pdc_name,
        from_ts,
        to_ts,
        jsonb_array_elements(arr) AS elem

    FROM src
),

pmu_ids AS
(
    -- ============================================================
    -- PMU representada diretamente como string
    -- ============================================================

    SELECT
        r.pdc_name,
        r.from_ts,
        r.to_ts,
        p.pmu_id,
        p.id_name

    FROM elems r

    JOIN openplot.pmu p
      ON p.id_name = btrim(r.elem::text, '""')

    WHERE jsonb_typeof(r.elem) = 'string'


    UNION ALL


    -- ============================================================
    -- PMU representada como objeto:
    -- { pmu: ... }
    -- { id_name: ... }
    -- ============================================================

    SELECT
        r.pdc_name,
        r.from_ts,
        r.to_ts,
        p.pmu_id,
        p.id_name

    FROM elems r

    JOIN LATERAL
    (
        SELECT
            NULLIF(TRIM(r.elem->>'pmu'), '')     AS key_pmu,
            NULLIF(TRIM(r.elem->>'id_name'), '') AS key_idname
    ) k ON TRUE

    JOIN openplot.pmu p
      ON p.id_name = COALESCE(k.key_pmu, k.key_idname)

    WHERE jsonb_typeof(r.elem) = 'object'
      AND COALESCE(k.key_pmu, k.key_idname) IS NOT NULL


    UNION ALL


    -- ============================================================
    -- PMU identificada por pdc_pmu_id
    -- ============================================================

    SELECT
        r.pdc_name,
        r.from_ts,
        r.to_ts,
        p.pmu_id,
        p.id_name

    FROM elems r

    JOIN LATERAL
    (
        SELECT
            NULLIF(r.elem->>'pdc_pmu_id', '')::int
                AS key_pdc_pmu_id
    ) k ON TRUE

    JOIN openplot.pdc_pmu ppm
      ON ppm.pdc_pmu_id = k.key_pdc_pmu_id

    JOIN openplot.pmu p
      ON p.pmu_id = ppm.pmu_id

    WHERE jsonb_typeof(r.elem) = 'object'


    UNION ALL


    -- ============================================================
    -- PMU identificada por signal_id
    -- ============================================================

    SELECT
        r.pdc_name,
        r.from_ts,
        r.to_ts,
        p.pmu_id,
        p.id_name

    FROM elems r

    JOIN LATERAL
    (
        SELECT
            NULLIF(r.elem->>'signal_id', '')::int
                AS key_signal_id
    ) k ON TRUE

    JOIN openplot.signal s
      ON s.signal_id = k.key_signal_id

    JOIN openplot.pdc_pmu ppm
      ON ppm.pdc_pmu_id = s.pdc_pmu_id

    JOIN openplot.pmu p
      ON p.pmu_id = ppm.pmu_id

    WHERE jsonb_typeof(r.elem) = 'object'
),

pdc_ctx AS
(
    SELECT
        w.pdc_name,
        w.from_ts,
        w.to_ts,
        pdc.pdc_id

    FROM src w

    JOIN openplot.pdc pdc
      ON LOWER(pdc.name) = LOWER(w.pdc_name)
),

ctx AS
(
    SELECT DISTINCT
        pc.pdc_name,
        pc.from_ts,
        pc.to_ts,
        pid.id_name,
        pid.pmu_id,
        pc.pdc_id

    FROM pdc_ctx pc

    JOIN pmu_ids pid
      ON pid.pdc_name = pc.pdc_name
),

sig AS
(
    SELECT DISTINCT
        s.signal_id,
        s.pdc_pmu_id,

        s.name AS signal_name,

        LOWER(s.quantity::text) AS quantity,

        UPPER(s.component::text) AS component,

        -- --------------------------------------------------------
        -- Fase original do catálogo.
        -- É ela que também determina a coluna da measurements.
        -- --------------------------------------------------------
        UPPER(s.phase::text) AS phase_raw,

        -- --------------------------------------------------------
        -- Fase que será enviada para o gerador COMTRADE.
        --
        -- Somente Voltage / Current e THD possuem fase.
        --
        -- A -> Am
        -- B -> Bm
        -- C -> Cm
        --
        -- Frequency / DFREQ / Digital => NULL
        -- --------------------------------------------------------
        CASE
            WHEN LOWER(s.quantity::text)
                     IN ('voltage', 'v', 'current', 'i')

                 AND UPPER(s.component::text)
                     IN ('MAG', 'ANG', 'THD')

                 AND UPPER(s.phase::text)
                     IN ('A', 'B', 'C')

            THEN
                CASE UPPER(s.phase::text)
                    WHEN 'A' THEN 'Am'
                    WHEN 'B' THEN 'Bm'
                    WHEN 'C' THEN 'Cm'
                END

            ELSE NULL
        END AS comtrade_phase,

        c.id_name,
        c.pdc_name

    FROM ctx c

    JOIN openplot.pdc_pmu pp
      ON pp.pdc_id = c.pdc_id
     AND pp.pmu_id = c.pmu_id

    JOIN openplot.signal s
      ON s.pdc_pmu_id = pp.pdc_pmu_id

    WHERE
    (
        -- ========================================================
        -- FASORES DE TENSÃO / CORRENTE
        -- ========================================================

        (
            LOWER(s.quantity::text)
                IN ('voltage', 'v', 'current', 'i')

            AND UPPER(s.phase::text)
                IN ('A', 'B', 'C')

            AND UPPER(s.component::text)
                IN ('MAG', 'ANG')
        )

        OR

        -- ========================================================
        -- FREQUÊNCIA / DELTA FREQUÊNCIA
        -- ========================================================

        (
            LOWER(s.quantity::text)
                IN ('frequency', 'freq')

            AND LOWER(s.component::text)
                IN ('freq', 'dfreq')
        )

        OR

        -- ========================================================
        -- THD
        -- ========================================================

        (
            LOWER(s.quantity::text)
                IN ('voltage', 'v', 'current', 'i')

            AND UPPER(s.phase::text)
                IN ('A', 'B', 'C')

            AND UPPER(s.component::text) = 'THD'
        )

        OR

        -- ========================================================
        -- DIGITAL
        -- ========================================================

        (
            LOWER(s.quantity::text)
                IN ('digital', 'd')

            OR UPPER(s.component::text)
                IN ('DIG', 'DIGITAL')
        )
    )
)

SELECT
    s.signal_id       AS SignalId,
    s.pdc_pmu_id      AS PdcPmuId,

    s.id_name         AS IdName,
    s.pdc_name        AS PdcName,

    s.signal_name     AS SignalName,
    s.quantity        AS Quantity,
    s.component       AS Component,

    -- Valor que será utilizado pelo COMTRADE:
    -- Am / Bm / Cm ou NULL
    s.comtrade_phase  AS Phase,

    NULL              AS Unit,

    mw.ts             AS Ts,

    {VALUE_CASE}      AS Value

FROM sig s

JOIN openplot.measurements mw
  ON mw.pdc_pmu_id = s.pdc_pmu_id

WHERE mw.ts >= (SELECT from_utc FROM win)
  AND mw.ts <= (SELECT to_utc   FROM win)

  AND ({VALUE_CASE}) IS NOT NULL

ORDER BY
    s.id_name,
    s.signal_id,
    mw.ts;
"
.Replace("{VALUE_CASE}", valueCase);

        var args = new
        {
            run_id = runId,
            from_utc = fromUtc,
            to_utc = toUtc,
            pmus = pmusOverride
        };

        var rows = await _db.Conn.QueryAsync<MeasurementRow>(
            new CommandDefinition(
                sql,
                args,
                commandTimeout: _queryTimeoutSeconds,
                cancellationToken: ct));

        return rows.AsList();
    }
}