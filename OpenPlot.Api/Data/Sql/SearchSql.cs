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

        // Buscas abaixo não levam o usuário. Ainda não sabemos se haverá filtro por user
        public const string ListRuns = @"
SELECT id, source, terminal_id, from_ts, to_ts, select_rate, status, created_at
FROM openplot.search_runs
WHERE (@status IS NULL OR status = @status)
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
