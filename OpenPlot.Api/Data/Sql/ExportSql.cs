namespace Data.Sql
{
    public static class ExportSql
    {
        public const string OwnedSearchRunExists = @"
SELECT id
FROM openplot.search_runs
WHERE id = @run_id
  AND LOWER(username) = LOWER(@username)
LIMIT 1;";

        public const string QueueExportRun = @"
INSERT INTO openplot.comtrade_runs
  (run_id, status, progress, message)
SELECT
  @run_id, 'queued', 0, 'Na fila'
WHERE NOT EXISTS (
  SELECT 1
  FROM openplot.comtrade_runs
  WHERE run_id = @run_id
);";

        public const string GetExportRunStatus = @"
SELECT
  c.run_id,
  'comtrade' AS format,
  c.status,
  c.progress,
  c.message,
  c.error,
  c.dir_path,
  c.file_name,
  c.size_bytes,
  c.sha256,
  c.created_at,
  c.started_at,
  c.finished_at
FROM openplot.comtrade_runs AS c
JOIN openplot.search_runs AS s
  ON s.id = c.run_id
WHERE c.run_id = @run_id
  AND LOWER(s.username) = LOWER(@username)
LIMIT 1;";
    }
}
