using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Features.Runs.Repositories;

public sealed record RunContext(
    Guid RunId,
    string PdcName,
    DateTime FromUtc,
    DateTime ToUtc,
    int PdcId,
    IReadOnlyList<string> PmuNames, // id_name
    int? SelectRate
);

public interface IRunContextRepository
{
    Task<RunContext?> ResolveAsync(Guid runId, DateTime? fromUtc, DateTime? toUtc, CancellationToken ct);
}

public sealed class RunContextRepository : IRunContextRepository
{
    private readonly IDbConnectionFactory _dbf;
    public RunContextRepository(IDbConnectionFactory dbf) => _dbf = dbf;

    public async Task<RunContext?> ResolveAsync(Guid runId, DateTime? fromUtc, DateTime? toUtc, CancellationToken ct)
    {
        using var db = _dbf.Create();

        const string sql = @"
WITH run AS (
  SELECT
    id,
    source AS pdc_name,
    from_ts,
    to_ts,
    select_rate, -- <<<<<<<<<<<<<<
    COALESCE(pmus_ok, pmus) AS pmus,
    signals
  FROM openplot.search_runs
  WHERE id = @run_id::uuid
),
run_window AS (
  SELECT
    CASE WHEN pg_typeof(r.from_ts)::text = 'timestamp without time zone'
         THEN r.from_ts::timestamptz ELSE r.from_ts END AS from_utc,
    CASE WHEN pg_typeof(r.to_ts)::text = 'timestamp without time zone'
         THEN r.to_ts::timestamptz ELSE r.to_ts   END AS to_utc,
    r.pdc_name, r.signals, r.pmus, r.select_rate
  FROM run r
),
win AS (
  SELECT
    COALESCE(@from_utc, rw.from_utc) AS from_utc,
    COALESCE(@to_utc,   rw.to_utc)   AS to_utc,
    rw.pdc_name, rw.signals, rw.pmus, rw.select_rate
  FROM run_window rw
),
src AS (
  SELECT w.pdc_name,
         w.from_utc AS from_ts,
         w.to_utc   AS to_ts,
         w.select_rate,
         CASE
           WHEN jsonb_typeof(w.signals) = 'array' AND jsonb_array_length(w.signals) > 0 THEN w.signals
           WHEN jsonb_typeof(w.pmus)    = 'array' AND jsonb_array_length(w.pmus)    > 0 THEN w.pmus
           ELSE '[]'::jsonb
         END AS arr
  FROM win w
),
elems AS (
  SELECT pdc_name, from_ts, to_ts, select_rate, jsonb_array_elements(arr) AS elem
  FROM src
),
pmu_names AS (
  SELECT DISTINCT p.id_name
  FROM elems e
  JOIN openplot.pmu p ON p.id_name = btrim(e.elem::text, '""')
  WHERE jsonb_typeof(e.elem) = 'string'

  UNION
  SELECT DISTINCT p.id_name
  FROM elems e
  JOIN LATERAL (
    SELECT NULLIF(TRIM(e.elem->>'pmu'), '')     AS key_pmu,
           NULLIF(TRIM(e.elem->>'id_name'), '') AS key_idname
  ) k ON TRUE
  JOIN openplot.pmu p ON p.id_name = COALESCE(k.key_pmu, k.key_idname)
  WHERE jsonb_typeof(e.elem) = 'object'
    AND COALESCE(k.key_pmu, k.key_idname) IS NOT NULL

  UNION
  SELECT DISTINCT p.id_name
  FROM elems e
  JOIN LATERAL (SELECT NULLIF(e.elem->>'pdc_pmu_id','')::int AS key_pdc_pmu_id) k ON TRUE
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(e.elem) = 'object'

  UNION
  SELECT DISTINCT p.id_name
  FROM elems e
  JOIN LATERAL (SELECT NULLIF(e.elem->>'signal_id','')::int AS key_signal_id) k ON TRUE
  JOIN openplot.signal s ON s.signal_id = k.key_signal_id
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(e.elem) = 'object'
),
pdc_ctx AS (
  SELECT w.pdc_name, w.from_ts, w.to_ts, pdc.pdc_id, w.select_rate
  FROM src w
  JOIN openplot.pdc pdc ON LOWER(pdc.name) = LOWER(w.pdc_name)
)
SELECT
  pc.pdc_name,
  pc.from_ts AS from_utc,
  pc.to_ts   AS to_utc,
  pc.pdc_id,
  pc.select_rate,
  (SELECT array_agg(id_name ORDER BY id_name) FROM pmu_names) AS pmus
FROM pdc_ctx pc;
";

        var row = await db.QueryFirstOrDefaultAsync<(string pdc_name, DateTime from_utc, DateTime to_utc, int pdc_id, int? select_rate, string[] pmus)>(
            new CommandDefinition(sql, new { run_id = runId, from_utc = fromUtc, to_utc = toUtc }, cancellationToken: ct));

        if (row.pdc_name is null) return null;

        return new RunContext(
            runId,
            row.pdc_name,
            row.from_utc,
            row.to_utc,
            row.pdc_id,
            row.pmus ?? Array.Empty<string>(),
            row.select_rate
        );
    }
}
