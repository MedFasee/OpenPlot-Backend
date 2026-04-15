namespace Data.Sql
{
    public static class SearchSql
    {
        public const string InsertRunBlind = @"
WITH d AS (
  SELECT pdc_id FROM openplot.pdc WHERE trim(name) = trim(@source) LIMIT 1
)
INSERT INTO openplot.search_runs
  (id, pdc_id, source, terminal_id, pmus, signals, from_ts, to_ts, select_rate,
   status, progress, message, pmu_count, label, username)
SELECT
  @id, d.pdc_id, @source, 'multi', @pmus::jsonb, '[]'::jsonb, @from, @to, @rate,
  'queued', 0, 'Na fila', @pmu_count, @label, @username
FROM d;";

        public const string SetRunShared = @"
UPDATE openplot.search_runs
SET shared = @shared
WHERE id = @id
  AND LOWER(username) = LOWER(@username)
RETURNING id, shared, username, created_at;";

        public const string SoftDeleteRun = @"
UPDATE openplot.search_runs
SET
  is_visible = @is_visible,
  deleted_at = CASE WHEN @is_visible = FALSE THEN now() ELSE NULL END
WHERE id = @id
  AND LOWER(username) = LOWER(@username)
RETURNING id, is_visible, deleted_at;";



        public const string ListRuns =  @"
SELECT
  s.id,
  s.source,
  s.terminal_id,
  s.from_ts,
  s.to_ts,
  s.select_rate,
  s.status,
  s.created_at,
  s.shared,
  s.username,

  (LOWER(s.username) = LOWER(@username)) AS owner,

  CASE
    WHEN c.run_id IS NULL THEN 'absent'
    WHEN LOWER(c.status) IN ('queued', 'running', 'failed', 'done') THEN LOWER(c.status)
    ELSE 'absent'
  END AS conv_comtrade

FROM openplot.search_runs AS s
LEFT JOIN openplot.comtrade_runs AS c
  ON c.run_id = s.id
WHERE
  ( @status IS NULL OR s.status = @status )
  AND
  ( s.shared = TRUE OR LOWER(s.username) = LOWER(@username) )
  AND
  ( s.is_visible = TRUE )
ORDER BY s.created_at DESC
LIMIT 5000;";


        public const string GetRunById = @"
SELECT id, source, terminal_id, signals::text, from_ts, to_ts, select_rate, status, created_at
FROM openplot.search_runs WHERE id=@id LIMIT 1;";

        public const string ListRecentDone = @"
SELECT id, source, terminal_id, signals::text, from_ts, to_ts, select_rate, status, created_at
FROM openplot.search_runs
WHERE status IN ('done','completed','finished')
ORDER BY created_at DESC
LIMIT 5000;";
    }
}
