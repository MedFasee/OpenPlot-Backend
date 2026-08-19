using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Npgsql;

namespace OpenPlot.Ingestor.Gsf.Hosting;

/// <summary>
/// Acumula as métricas do job em memória e publica snapshots incrementais em
/// openplot.search_runs.message.
///
/// Cada chunk concluído gera um novo snapshot contendo:
/// - todos os chunks concluídos até então;
/// - totais acumulados por PMU;
/// - total acumulado do job.
///
/// A classe NÃO calcula frames/qualidade. Ela somente agrega métricas calculadas
/// no IngestorChunkPipeline e tempos de PMU medidos no IngestorJobProcessor.
/// </summary>
internal sealed class IngestorProgressReporter
{
    private readonly string _connString;
    private readonly Guid _jobId;
    private readonly int _totalChunks;
    private readonly int _chunksPerPmu;
    private readonly object _sync = new();

    private readonly Dictionary<string, ChunkIngestMetrics> _chunks =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, TimeSpan> _pmuTimes =
        new(StringComparer.OrdinalIgnoreCase);

    private long _done;

    public IngestorProgressReporter(
        string connString,
        Guid jobId,
        int totalChunks,
        int chunksPerPmu)
    {
        _connString = connString;
        _jobId = jobId;
        _totalChunks = Math.Max(1, totalChunks);
        _chunksPerPmu = Math.Max(1, chunksPerPmu);
    }

    /// <summary>
    /// Registra um chunk concluído e publica imediatamente o snapshot acumulado.
    /// Um mesmo chunk, se informado novamente, substitui sua métrica sem avançar
    /// o contador de progresso pela segunda vez.
    /// </summary>
    public int CurrentProgressPercent =>
        CalculateRunningPercent(Interlocked.Read(ref _done));


    public void CompleteChunk(ChunkIngestMetrics metrics)
    {
        string message;
        int pct;
        long done;

        lock (_sync)
        {
            var isNew = !_chunks.ContainsKey(metrics.Key);
            _chunks[metrics.Key] = metrics;

            if (isNew)
                Interlocked.Increment(ref _done);

            done = Interlocked.Read(ref _done);
            pct = CalculateRunningPercent(done);
            message = BuildMessageNoLock(
                jobProcessingTime: null,
                finalStatus: "running",
                details: null);
        }

        WriteRunningProgress(pct, message);
    }

    /// <summary>
    /// Registra um tick de progresso com mensagem de status (usado para logging de etapas intermediárias).
    /// Não incrementa o contador de chunks concluídos.
    /// </summary>
    public void Tick(string statusMessage)
    {
        // Simples registero de progresso intermoduário sem alterar métricas
        // Pode ser expandido para logging agregado se necessário
        Console.WriteLine($"[Progress] {statusMessage}");
    }

    /// <summary>
    /// Registra o wall-clock real da PMU. Não soma tempos dos chunks, pois os
    /// chunks podem executar em paralelo.
    /// </summary>
    public void CompletePmu(string pmu, TimeSpan processingTime)
    {
        if (string.IsNullOrWhiteSpace(pmu))
            return;

        string message;
        int pct;

        lock (_sync)
        {
            _pmuTimes[pmu] = processingTime;
            pct = CalculateRunningPercent(Interlocked.Read(ref _done));
            message = BuildMessageNoLock(
                jobProcessingTime: null,
                finalStatus: "running",
                details: null);
        }

        WriteRunningProgress(pct, message);
    }

    /// <summary>
    /// Gera a mensagem final preservando todas as métricas já coletadas.
    /// </summary>
    public string BuildFinalMessage(
        TimeSpan jobProcessingTime,
        string finalStatus,
        string? details = null)
    {
        lock (_sync)
        {
            return BuildMessageNoLock(
                jobProcessingTime,
                finalStatus,
                details);
        }
    }

    /// <summary>
    /// Gera um snapshot hierárquico e estruturado do progresso atual da ingestão.
    /// Estrutura: Job (contexto geral) → PMUs (agrupamento) → Chunks (detalhes por intervalo).
    /// Evita duplicação de informações gerais, otimizando para consumo frontend.
    /// </summary>
    public JobIngestProgressDto GetProgressSnapshot(
        Guid jobId,
        string jobSource,
        DateTime jobFromUtc,
        DateTime jobToUtc,
        int selectRate)
    {
        lock (_sync)
        {
            var completeChunks = Interlocked.Read(ref _done);
            var progressPct = CalculateRunningPercent(completeChunks);

            var dto = new JobIngestProgressDto
            {
                JobId = jobId.ToString("D"),
                JobSource = jobSource,
                JobFromUtc = jobFromUtc,
                JobToUtc = jobToUtc,
                SelectRate = selectRate,
                Status = "running",
                TotalChunks = _totalChunks,
                ChunksCompleted = (int)completeChunks,
                ProgressPercent = progressPct,
                Timestamp = DateTime.UtcNow,
                Pmus = new()
            };

            // Agrupar chunks por PMU e construir objetos PmuIngestProgressDto
            var pmuNames = _chunks.Values
                .Select(x => x.Pmu)
                .Concat(_pmuTimes.Keys)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var pmuName in pmuNames)
            {
                var pmuChunks = _chunks.Values
                    .Where(x => string.Equals(x.Pmu, pmuName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(x => x.FromUtc)
                    .ThenBy(x => x.ToUtc)
                    .ToList();

                if (pmuChunks.Count == 0)
                    continue;

                var pmuDto = new PmuIngestProgressDto
                {
                    PmuId = pmuName,
                    TerminalId = pmuChunks.First().Terminal,
                    ChunksCompleted = pmuChunks.Count,
                    Chunks = pmuChunks
                        .Select(SimpleChunkIngestMetricsDto.FromMetrics)
                        .ToList(),
                    TotalProcessingTimeMs = pmuChunks
                        .Sum(x => x.ProcessingTime.TotalMilliseconds),
                    TotalFramesExpected = pmuChunks
                        .Sum(x => x.ExpectedFrames),
                    TotalFramesPresent = pmuChunks
                        .Where(x => x.PresentFrames.HasValue)
                        .Sum(x => x.PresentFrames ?? 0),
                    Status = pmuChunks.All(x => x.Status == "done") ? "done" : "running"
                };

                dto.Pmus.Add(pmuDto);
            }

            return dto;
        }
    }

    private int CalculateRunningPercent(long done)
    {
        var pct = (int)Math.Floor(100.0 * done / _totalChunks);
        return Math.Clamp(pct, 0, 99);
    }

    private string BuildMessageNoLock(
        TimeSpan? jobProcessingTime,
        string finalStatus,
        string? details)
    {
        var sb = new StringBuilder(4096);

        var completedChunks = _chunks.Count;
        var runningPct = CalculateRunningPercent(completedChunks);
        var isSuccessfulFinal =
            string.Equals(finalStatus, "done", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(finalStatus, "no_data", StringComparison.OrdinalIgnoreCase);

        var displayPct = isSuccessfulFinal ? 100 : runningPct;

        sb.Append("JOB ")
          .Append(_jobId)
          .Append(" | status=")
          .Append(finalStatus)
          .Append(" | progresso=")
          .Append(completedChunks)
          .Append('/')
          .Append(_totalChunks)
          .Append(" (")
          .Append(displayPct)
          .AppendLine("%)");

        if (jobProcessingTime.HasValue)
        {
            sb.Append("tempo_job=")
              .Append(FormatDuration(jobProcessingTime.Value))
              .AppendLine();
        }

        if (!string.IsNullOrWhiteSpace(details))
        {
            sb.Append("detalhes=")
              .AppendLine(Sanitize(details));
        }

        var pmuNames = _chunks.Values
            .Select(x => x.Pmu)
            .Concat(_pmuTimes.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        foreach (var pmu in pmuNames)
        {
            var pmuChunks = _chunks.Values
                .Where(x => string.Equals(x.Pmu, pmu, StringComparison.OrdinalIgnoreCase))
                .OrderBy(x => x.FromUtc)
                .ThenBy(x => x.ToUtc)
                .ToArray();

            sb.AppendLine();
            sb.Append("PMU ").Append(pmu);

            if (_pmuTimes.TryGetValue(pmu, out var pmuTime))
                sb.Append(" | tempo=").Append(FormatDuration(pmuTime));
            else
                sb.Append(" | tempo=em_processamento");

            sb.Append(" | chunks=")
              .Append(pmuChunks.Length)
              .Append('/')
              .Append(_chunksPerPmu)
              .AppendLine();

            foreach (var chunk in pmuChunks)
            {
                sb.Append("  CHUNK ")
                  .Append(chunk.FromUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff'Z'"))
                  .Append(" -> ")
                  .Append(chunk.ToUtc.ToString("yyyy-MM-ddTHH:mm:ss.fff'Z'"))
                  .Append(" | status=")
                  .Append(chunk.Status)
                  .Append(" | tempo=")
                  .Append(FormatDuration(chunk.ProcessingTime))
                  .Append(" | esperados=")
                  .Append(chunk.ExpectedFrames)
                  .Append(" | presentes=")
                  .Append(FormatNullable(chunk.PresentFrames))
                  .Append(" | faltantes=")
                  .Append(FormatNullable(chunk.MissingFrames))
                  .Append(" | qualidade_ruim=")
                  .Append(FormatNullable(chunk.BadQualityFrames));

                if (!string.IsNullOrWhiteSpace(chunk.Details))
                {
                    sb.Append(" | detalhe=")
                      .Append(Sanitize(chunk.Details));
                }

                sb.AppendLine();
            }

            AppendPmuTotals(sb, pmuChunks);
        }

        sb.AppendLine();
        AppendJobTotals(sb, jobProcessingTime);

        return sb.ToString().TrimEnd();
    }

    private static void AppendPmuTotals(
        StringBuilder sb,
        IReadOnlyCollection<ChunkIngestMetrics> chunks)
    {
        var expected = chunks.Sum(x => (long)x.ExpectedFrames);
        var knownPresent = chunks.Where(x => x.PresentFrames.HasValue).ToArray();
        var knownMissing = chunks.Where(x => x.MissingFrames.HasValue).ToArray();
        var knownBad = chunks.Where(x => x.BadQualityFrames.HasValue).ToArray();

        sb.Append("  TOTAL PMU")
          .Append(" | esperados=").Append(expected)
          .Append(" | presentes=").Append(FormatAggregate(knownPresent.Length, chunks.Count, knownPresent.Sum(x => (long)x.PresentFrames!.Value)))
          .Append(" | faltantes=").Append(FormatAggregate(knownMissing.Length, chunks.Count, knownMissing.Sum(x => (long)x.MissingFrames!.Value)))
          .Append(" | qualidade_ruim=").Append(FormatAggregate(knownBad.Length, chunks.Count, knownBad.Sum(x => (long)x.BadQualityFrames!.Value)))
          .AppendLine();
    }

    private void AppendJobTotals(StringBuilder sb, TimeSpan? jobProcessingTime)
    {
        var chunks = _chunks.Values.ToArray();
        var expected = chunks.Sum(x => (long)x.ExpectedFrames);
        var knownPresent = chunks.Where(x => x.PresentFrames.HasValue).ToArray();
        var knownMissing = chunks.Where(x => x.MissingFrames.HasValue).ToArray();
        var knownBad = chunks.Where(x => x.BadQualityFrames.HasValue).ToArray();
        var pmus = chunks.Select(x => x.Pmu)
            .Concat(_pmuTimes.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();

        sb.Append("TOTAL JOB")
          .Append(" | pmus=").Append(pmus)
          .Append(" | chunks=").Append(chunks.Length).Append('/').Append(_totalChunks);

        if (jobProcessingTime.HasValue)
            sb.Append(" | tempo=").Append(FormatDuration(jobProcessingTime.Value));

        sb.Append(" | esperados=").Append(expected)
          .Append(" | presentes=").Append(FormatAggregate(knownPresent.Length, chunks.Length, knownPresent.Sum(x => (long)x.PresentFrames!.Value)))
          .Append(" | faltantes=").Append(FormatAggregate(knownMissing.Length, chunks.Length, knownMissing.Sum(x => (long)x.MissingFrames!.Value)))
          .Append(" | qualidade_ruim=").Append(FormatAggregate(knownBad.Length, chunks.Length, knownBad.Sum(x => (long)x.BadQualityFrames!.Value)))
          .AppendLine();
    }

    private static string FormatAggregate(int known, int total, long value)
    {
        if (total == 0)
            return "0";

        return known == total
            ? value.ToString()
            : $"{value} (parcial {known}/{total} chunks)";
    }

    private static string FormatNullable(int? value) =>
        value.HasValue ? value.Value.ToString() : "n/d";

    private static string FormatDuration(TimeSpan value)
    {
        if (value.TotalMinutes >= 1)
            return $"{(int)value.TotalMinutes}m{value.Seconds:D2}.{value.Milliseconds:D3}s";

        return $"{value.TotalSeconds:F3}s";
    }

    private static string Sanitize(string value) =>
        value.Replace('\r', ' ').Replace('\n', ' ').Trim();

    private void WriteRunningProgress(int pct, string message)
    {
        try
        {
            using var conn = new NpgsqlConnection(_connString);
            conn.Open();
            using var tx = conn.BeginTransaction();
            DbOps.UpdateStatus(conn, tx, _jobId, "running", pct, message);
            tx.Commit();
        }
        catch
        {
            // Progresso/telemetria não deve interromper a ingestão.
        }
    }
}
