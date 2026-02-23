using System.ComponentModel;
using System.Numerics;
using System.Xml.Linq;
using Dapper;
using Microsoft.Extensions.Logging;
using OpenPlot.ExportWorker.Domain;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace OpenPlot.ExportWorker.Data;

public sealed class MeasurementsRepo
{
    private readonly Db _db;
    public MeasurementsRepo(Db db) => _db = db;

    /// <summary>
    /// Resolve a seleção (PMUs) a partir do run_id, encontra signals (inclui MAG/ANG/FREQ/THD/DIGITAL),
    /// busca os pontos por (signal_id, ts) e retorna rows ricos para montagem de COMTRADE.
    /// 
    /// IMPORTANTe: openplot.measurements NÃO tem run_id.
    /// </summary>
    public async Task<List<MeasurementRow>> LoadMeasurementsForComtradeAsync(
        Guid runId,
        DateTimeOffset? fromUtc,
        DateTimeOffset? toUtc,
        string[]? pmusOverride,
        CancellationToken ct)
    {
        const string sql = @"
WITH run AS (
  SELECT id, source AS pdc_name, from_ts, to_ts, COALESCE(pmus_ok, pmus) AS pmus, signals
  FROM openplot.search_runs
  WHERE id = @run_id::uuid
),
run_window AS (
  SELECT
    CASE WHEN pg_typeof(r.from_ts)::text = 'timestamp without time zone'
         THEN r.from_ts::timestamptz ELSE r.from_ts END AS from_utc,
    CASE WHEN pg_typeof(r.to_ts)::text = 'timestamp without time zone'
         THEN r.to_ts::timestamptz ELSE r.to_ts   END AS to_utc,
    r.pdc_name, r.signals, r.pmus
  FROM run r
),
win AS (
  SELECT
    COALESCE(@from_utc, rw.from_utc) AS from_utc,
    COALESCE(@to_utc,   rw.to_utc)   AS to_utc,
    rw.pdc_name, rw.signals, rw.pmus
  FROM run_window rw
),
src AS (
  SELECT w.pdc_name,
         w.from_utc AS from_ts,
         w.to_utc   AS to_ts,
         CASE
           WHEN @pmus IS NOT NULL THEN to_jsonb(@pmus::text[])
           WHEN jsonb_typeof(w.pmus) = 'array' AND jsonb_array_length(w.pmus) > 0 THEN w.pmus
           ELSE '[]'::jsonb
         END AS arr
  FROM win w
),
elems AS (
  SELECT pdc_name, from_ts, to_ts, jsonb_array_elements(arr) AS elem
  FROM src
),
pmu_ids AS (
  -- PMUs vindas como string: [UF27, UF28, ...]
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN openplot.pmu p ON p.id_name = btrim(r.elem::text, '""')
  WHERE jsonb_typeof(r.elem) = 'string'

  UNION ALL
  --PMUs vindas como objeto com chaves pmu/ id_name
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL(
    SELECT NULLIF(TRIM(r.elem->> 'pmu'), '')     AS key_pmu,
           NULLIF(TRIM(r.elem->> 'id_name'), '') AS key_idname
  ) k ON TRUE
  JOIN openplot.pmu p ON p.id_name = COALESCE(k.key_pmu, k.key_idname)
  WHERE jsonb_typeof(r.elem) = 'object'
    AND COALESCE(k.key_pmu, k.key_idname) IS NOT NULL

  UNION ALL
  -- PMU via pdc_pmu_id
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL(SELECT NULLIF(r.elem->> 'pdc_pmu_id','')::int AS key_pdc_pmu_id) k ON TRUE
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(r.elem) = 'object'

  UNION ALL
  --PMU via signal_id
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL(SELECT NULLIF(r.elem->> 'signal_id','')::int AS key_signal_id) k ON TRUE
  JOIN openplot.signal s ON s.signal_id = k.key_signal_id
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(r.elem) = 'object'
),
pdc_ctx AS(
  SELECT w.pdc_name, w.from_ts, w.to_ts, pdc.pdc_id
  FROM src w
  JOIN openplot.pdc pdc ON LOWER(pdc.name) = LOWER(w.pdc_name)
),
ctx AS(
  SELECT pc.pdc_name, pc.from_ts, pc.to_ts, pid.id_name, pid.pmu_id, pc.pdc_id
  FROM pdc_ctx pc
  JOIN pmu_ids pid ON pid.pdc_name = pc.pdc_name
),
sig AS(
  SELECT
    s.signal_id,
    s.pdc_pmu_id,
    LOWER(s.quantity::text) AS quantity,
    UPPER(s.component::text) AS component,
    UPPER(s.phase::text) AS phase,
    c.id_name,
    c.pdc_name
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE
    (
      --Fasores V / I MAG / ANG ABC
      (LOWER(s.quantity::text) IN('voltage', 'v', 'current', 'i')
       AND UPPER(s.phase::text) IN('A', 'B', 'C')
       AND UPPER(s.component::text) IN('MAG', 'ANG'))

      OR

      -- Frequência
      (LOWER(s.quantity::text) IN('frequency', 'freq')
       AND LOWER(s.component::text) IN('freq'))

      OR

      -- THD(V / I por fase)
      (LOWER(s.quantity::text) IN('voltage', 'v', 'current', 'i')
       AND UPPER(s.phase::text) IN('A', 'B', 'C')
       AND LOWER(s.component::text) = 'thd')

      OR

      -- DIGITAIS(AJUSTE se seu schema usar outro padrão)
      (LOWER(s.quantity::text) IN('digital', 'd')
       OR UPPER(s.component::text) IN('DIG', 'DIGITAL'))
    )
),
sig_ids AS(
  SELECT DISTINCT signal_id FROM sig
),
raw AS(
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  JOIN sig_ids si ON si.signal_id = m.signal_id
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc FROM win)
)
SELECT
  s.signal_id AS SignalId,
  s.pdc_pmu_id AS PdcPmuId,
  s.id_name AS IdName,
  s.pdc_name AS PdcName,
  s.quantity AS Quantity,
  s.component AS Component,
  s.phase AS Phase,
  NULL AS Unit,
  r.ts AS Ts,
  r.value AS Value
FROM sig s
JOIN raw r USING(signal_id)
ORDER BY s.id_name, s.signal_id, r.ts;
        ";

        var args = new
        {
            run_id = runId,
            from_utc = fromUtc,
            to_utc = toUtc,
            pmus = pmusOverride
        };

        var rows = await _db.Conn.QueryAsync<MeasurementRow>(
            new CommandDefinition(sql, args, cancellationToken: ct));

        return rows.AsList();
    }
}