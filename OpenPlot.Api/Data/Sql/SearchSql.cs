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



        public const string ListRuns = @"
SELECT
  id, source, terminal_id, from_ts, to_ts, select_rate, status, created_at, shared, username
FROM openplot.search_runs
WHERE
  ( @status IS NULL OR status = @status )
  AND
  ( shared = TRUE OR LOWER(username) = LOWER(@username) )
AND 
(is_visible = TRUE)
ORDER BY created_at DESC
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
