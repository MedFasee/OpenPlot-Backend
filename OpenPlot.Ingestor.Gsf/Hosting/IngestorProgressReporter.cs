using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Npgsql;

namespace OpenPlot.Ingestor.Gsf.Hosting;

/// <summary>
/// Acumula as métricas técnicas por chunk, mas publica em search_runs.message
/// uma visão sintética orientada ao usuário.
///
/// Regras de apresentação:
/// - a PMU atualmente em processamento sempre aparece com métricas parciais;
/// - PMUs concluídas sem faltantes/qualidade ruim deixam de ocupar a lista;
/// - PMUs concluídas com ocorrências permanecem visíveis;
/// - a ordem visual das PMUs é fixa pela primeira aparição;
/// - novas PMUs são sempre adicionadas ao fim da lista visível;
/// - métricas/faltantes/qualidade nunca reordenam linhas já exibidas;
/// - o resumo parcial/global é atualizado incrementalmente a cada chunk;
/// - detalhes individuais de chunk NÃO são publicados na message.
/// </summary>
internal sealed class IngestorProgressReporter
{
    private static readonly CultureInfo PtBr = CultureInfo.GetCultureInfo("pt-BR");

    private readonly string _connString;
    private readonly Guid _jobId;
    private readonly int _totalChunks;
    private readonly int _totalPmus;
    private readonly int _chunksPerPmu;
    private readonly object _sync = new();
    private readonly long _createdTick;

    private readonly Dictionary<string, ChunkIngestMetrics> _chunks =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, TimeSpan> _pmuTimes =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, long> _pmuStartedTicks =
        new(StringComparer.OrdinalIgnoreCase);

    // Ordem estável de primeira aparição no relatório.
    // Uma vez atribuída, nunca muda durante a vida do job.
    private readonly Dictionary<string, long> _pmuOrder =
        new(StringComparer.OrdinalIgnoreCase);

    private long _nextPmuOrder;
    private long _done;

    private sealed record PmuAggregate(
        string Pmu,
        bool IsCompleted,
        bool IsActive,
        TimeSpan ProcessingTime,
        int CompletedChunks,
        long Expected,
        long Present,
        long Missing,
        long BadQuality,
        bool MetricsComplete,
        bool HasTechnicalIssue,
        bool HasOccurrences);

    public IngestorProgressReporter(
        string connString,
        Guid jobId,
        int totalChunks,
        int totalPmus,
        int chunksPerPmu)
    {
        _connString = connString;
        _jobId = jobId;
        _totalChunks = Math.Max(1, totalChunks);
        _totalPmus = Math.Max(1, totalPmus);
        _chunksPerPmu = Math.Max(1, chunksPerPmu);
        _createdTick = Stopwatch.GetTimestamp();
    }

    public int CurrentProgressPercent =>
        CalculateRunningPercent(Interlocked.Read(ref _done));

    /// <summary>
    /// Torna a PMU visível imediatamente no polling, antes mesmo do primeiro
    /// chunk terminar. Enquanto ativa, o tempo exibido é o tempo decorrido.
    /// </summary>
    public void StartPmu(string pmu)
    {
        if (string.IsNullOrWhiteSpace(pmu))
            return;

        string message;
        int pct;

        lock (_sync)
        {
            EnsurePmuOrderNoLock(pmu);

            if (!_pmuStartedTicks.ContainsKey(pmu))
                _pmuStartedTicks[pmu] = Stopwatch.GetTimestamp();

            pct = CalculateRunningPercent(Interlocked.Read(ref _done));
            message = BuildMessageNoLock(
                jobProcessingTime: null,
                finalStatus: "running",
                details: null);
        }

        WriteRunningProgress(pct, message);
    }

    /// <summary>
    /// Registra um chunk concluído. As métricas do chunk permanecem internas;
    /// a message é reconstruída como agregado por PMU + resumo do job.
    /// </summary>
    public void CompleteChunk(ChunkIngestMetrics metrics)
    {
        string message;
        int pct;

        lock (_sync)
        {
            EnsurePmuOrderNoLock(metrics.Pmu);

            var isNew = !_chunks.ContainsKey(metrics.Key);
            _chunks[metrics.Key] = metrics;

            if (isNew)
                Interlocked.Increment(ref _done);

            pct = CalculateRunningPercent(Interlocked.Read(ref _done));
            message = BuildMessageNoLock(
                jobProcessingTime: null,
                finalStatus: "running",
                details: null);
        }

        Console.WriteLine(
            $"[metric] chunk {metrics.Pmu} {metrics.FromUtc:HH:mm:ss}-{metrics.ToUtc:HH:mm:ss} " +
            $"status={metrics.Status} frames={metrics.PresentFrames?.ToString() ?? "n/d"}/{metrics.ExpectedFrames} " +
            $"missing={metrics.MissingFrames?.ToString() ?? "n/d"} badq={metrics.BadQualityFrames?.ToString() ?? "n/d"}");

        WriteRunningProgress(pct, message);
    }

    /// <summary>
    /// Registra o wall-clock real da PMU. O tempo NÃO é a soma dos tempos dos
    /// chunks, porque os chunks podem ter sido processados em paralelo.
    /// </summary>
    public void CompletePmu(string pmu, TimeSpan processingTime)
    {
        if (string.IsNullOrWhiteSpace(pmu))
            return;

        string message;
        int pct;

        lock (_sync)
        {
            EnsurePmuOrderNoLock(pmu);

            _pmuTimes[pmu] = processingTime;
            _pmuStartedTicks.Remove(pmu);

            pct = CalculateRunningPercent(Interlocked.Read(ref _done));
            message = BuildMessageNoLock(
                jobProcessingTime: null,
                finalStatus: "running",
                details: null);
        }

        WriteRunningProgress(pct, message);
    }

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

        var chunksDone = _chunks.Count;
        var runningPct = CalculateRunningPercent(chunksDone);
        var successfulFinal = IsSuccessfulFinal(finalStatus);
        var isRunning = string.Equals(finalStatus, "running", StringComparison.OrdinalIgnoreCase);

        if (isRunning)
        {
            sb.Append("PROCESSANDO | ")
              .Append(chunksDone)
              .Append('/')
              .Append(_totalChunks)
              .Append(" chunks (")
              .Append(runningPct)
              .Append("%) | tempo decorrido=")
              .Append(FormatDuration(GetElapsedSince(_createdTick)))
              .AppendLine();
        }
        else if (successfulFinal)
        {
            sb.Append("PROCESSAMENTO CONCLUÍDO");

            if (jobProcessingTime.HasValue)
                sb.Append(" | tempo total=").Append(FormatDuration(jobProcessingTime.Value));

            sb.AppendLine();
        }
        else
        {
            sb.Append("PROCESSAMENTO FINALIZADO | status=")
              .Append(finalStatus);

            if (jobProcessingTime.HasValue)
                sb.Append(" | tempo total=").Append(FormatDuration(jobProcessingTime.Value));

            sb.AppendLine();
        }

        if (!string.IsNullOrWhiteSpace(details))
        {
            sb.Append("Detalhes: ")
              .AppendLine(Sanitize(details));
        }

        var aggregates = GetAllPmuAggregatesNoLock();

        // A lista visível é sempre ordenada pela PRIMEIRA APARIÇÃO da PMU.
        //
        // Consequências:
        // - uma nova PMU entra sempre no fim;
        // - uma PMU já exibida nunca sobe/desce por quantidade de faltantes;
        // - uma PMU ativa que termina com ocorrência permanece na mesma posição;
        // - uma PMU concluída sem ocorrência pode sair da lista, reduzindo ruído.
        var visible = aggregates
            .Where(x =>
                x.IsActive ||
                (x.IsCompleted && (x.HasOccurrences || x.HasTechnicalIssue)))
            .OrderBy(x => GetPmuOrderNoLock(x.Pmu))
            .ToArray();

        var completedWithoutData = aggregates.Count(x =>
            x.IsCompleted && IsPmuWithoutData(x));

        sb.AppendLine();
        sb.AppendLine("PMUs");

        if (visible.Length == 0)
        {
            sb.AppendLine("Nenhuma PMU em processamento ou com ocorrência até o momento.");
        }
        else
        {
            foreach (var pmu in visible)
                AppendPmuLine(sb, pmu);
        }

        var completedPmus = aggregates.Count(x => x.IsCompleted);
        var completedWithData = aggregates.Count(x =>
            x.IsCompleted && x.Present > 0);

        sb.Append("PMUs concluídas=")
          .Append(completedPmus)
          .Append('/')
          .Append(_totalPmus)
          .Append(" | sem dados=")
          .Append(completedWithoutData)
          .Append(" | com dados=")
          .Append(completedWithData)
          .AppendLine();

        sb.AppendLine();
        AppendJobSummary(sb, jobProcessingTime, finalStatus, aggregates);

        return sb.ToString().TrimEnd();
    }

    private void AppendPmuLine(StringBuilder sb, PmuAggregate pmu)
    {
        sb.Append(pmu.Pmu)
          .Append(" | tempo=");

        if (pmu.IsCompleted)
        {
            sb.Append(FormatDuration(pmu.ProcessingTime));
        }
        else
        {
            sb.Append(FormatDuration(pmu.ProcessingTime))
              .Append(" (em processamento)");
        }

        sb.Append(" | esperados=")
          .Append(FormatNumber(pmu.Expected))
          .Append(" | presentes=")
          .Append(FormatMetricWithPercent(
              pmu.Present,
              pmu.Expected,
              pmu.MetricsComplete,
              "dos esperados"))
          .Append(" | faltantes=")
          .Append(FormatMetricWithPercent(
              pmu.Missing,
              pmu.Expected,
              pmu.MetricsComplete,
              "dos esperados"))
          .Append(" | qualidade ruim=")
          .Append(FormatMetricWithPercent(
              pmu.BadQuality,
              pmu.Present,
              pmu.MetricsComplete,
              "dos presentes"));

        if (pmu.HasTechnicalIssue)
            sb.Append(" | atenção=métrica incompleta/falha técnica");

        sb.AppendLine();
        sb.AppendLine();
    }

    private void AppendJobSummary(
        StringBuilder sb,
        TimeSpan? jobProcessingTime,
        string finalStatus,
        IReadOnlyCollection<PmuAggregate> aggregates)
    {
        var chunks = _chunks.Values.ToArray();
        var normalized = chunks.Select(NormalizeChunkMetrics).ToArray();

        var expected = chunks.Sum(x => (long)x.ExpectedFrames);
        var present = normalized.Where(x => x.Present.HasValue).Sum(x => (long)x.Present!.Value);
        var missing = normalized.Where(x => x.Missing.HasValue).Sum(x => (long)x.Missing!.Value);
        var bad = normalized.Where(x => x.BadQuality.HasValue).Sum(x => (long)x.BadQuality!.Value);

        var completeChunks = normalized.Count(x => x.IsComplete);
        var metricsComplete =
            chunks.Length == _totalChunks &&
            completeChunks == chunks.Length;

        var title = string.Equals(finalStatus, "running", StringComparison.OrdinalIgnoreCase)
            ? "RESUMO PARCIAL"
            : "RESUMO";

        sb.Append(title)
          .Append(" | PMUs=")
          .Append(aggregates.Count(x => x.IsCompleted))
          .Append('/')
          .Append(_totalPmus)
          .Append(" | chunks=")
          .Append(chunks.Length)
          .Append('/')
          .Append(_totalChunks);

        if (!string.Equals(finalStatus, "running", StringComparison.OrdinalIgnoreCase) &&
            jobProcessingTime.HasValue)
        {
            // O tempo já aparece no cabeçalho final. Não o repetimos aqui.
        }

        sb.AppendLine();

        sb.Append("Esperados=")
          .Append(FormatNumber(expected))
          .Append(" | presentes=")
          .Append(FormatMetricWithPercent(present, expected, metricsComplete, "dos esperados"))
          .Append(" | faltantes=")
          .Append(FormatMetricWithPercent(missing, expected, metricsComplete, "dos esperados"))
          .Append(" | qualidade ruim=")
          .Append(FormatMetricWithPercent(bad, present, metricsComplete, "dos presentes"))
          .AppendLine()
          .AppendLine();

        if (!metricsComplete)
        {
            sb.Append("Métricas consolidadas em ")
              .Append(completeChunks)
              .Append('/')
              .Append(_totalChunks)
              .AppendLine(" chunks concluídos até o momento.");
        }
    }

    private PmuAggregate[] GetAllPmuAggregatesNoLock()
    {
        var pmuNames = _chunks.Values
            .Select(x => x.Pmu)
            .Concat(_pmuTimes.Keys)
            .Concat(_pmuStartedTicks.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return pmuNames
            .Select(GetPmuAggregateNoLock)
            .ToArray();
    }

    private PmuAggregate GetPmuAggregateNoLock(string pmu)
    {
        var chunks = _chunks.Values
            .Where(x => string.Equals(x.Pmu, pmu, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        var normalized = chunks.Select(NormalizeChunkMetrics).ToArray();
        var expected = chunks.Sum(x => (long)x.ExpectedFrames);

        var isCompleted = _pmuTimes.TryGetValue(pmu, out var finalTime);
        var isActive = _pmuStartedTicks.TryGetValue(pmu, out var startTick);

        var processingTime = isCompleted
            ? finalTime
            : isActive
                ? GetElapsedSince(startTick)
                : TimeSpan.Zero;

        // Para PMU em processamento, os números são necessariamente parciais,
        // mesmo que todos os chunks já recebidos tenham métricas válidas.
        var metricsComplete =
            isCompleted &&
            chunks.Length == _chunksPerPmu &&
            normalized.All(x => x.IsComplete);

        var present = normalized.Where(x => x.Present.HasValue).Sum(x => (long)x.Present!.Value);
        var missing = normalized.Where(x => x.Missing.HasValue).Sum(x => (long)x.Missing!.Value);
        var bad = normalized.Where(x => x.BadQuality.HasValue).Sum(x => (long)x.BadQuality!.Value);

        var isNoDataPmu = expected > 0 &&
                          present == 0 &&
                          missing == expected &&
                          bad == 0;

        var hasTechnicalIssue =
            !isNoDataPmu &&
            (chunks.Any(x => !IsNormalChunkStatus(x.Status)) ||
             (isCompleted && chunks.Length != _chunksPerPmu) ||
             (chunks.Length > 0 && !metricsComplete && isCompleted));

        return new PmuAggregate(
            Pmu: pmu,
            IsCompleted: isCompleted,
            IsActive: isActive,
            ProcessingTime: processingTime,
            CompletedChunks: chunks.Length,
            Expected: expected,
            Present: present,
            Missing: missing,
            BadQuality: bad,
            MetricsComplete: metricsComplete,
            HasTechnicalIssue: hasTechnicalIssue,
            HasOccurrences: missing > 0 || bad > 0);
    }

    private static (int? Present, int? Missing, int? BadQuality, bool IsComplete) NormalizeChunkMetrics(ChunkIngestMetrics chunk)
    {
        var present = chunk.PresentFrames;
        var missing = chunk.MissingFrames;
        var badQuality = chunk.BadQualityFrames;

        var dataAbsence = IsNoDataChunkStatus(chunk.Status) ||
                          IsTotalDataAbsenceChunk(chunk.ExpectedFrames, present, missing, badQuality);

        if (dataAbsence)
        {
            present = 0;
            missing = chunk.ExpectedFrames;
            badQuality = 0;
        }

        var complete = present.HasValue && missing.HasValue && badQuality.HasValue;
        return (present, missing, badQuality, complete);
    }

    private static bool IsTotalDataAbsenceChunk(
        int expectedFrames,
        int? present,
        int? missing,
        int? badQuality)
    {
        if (expectedFrames <= 0)
            return false;

        var presentZero = !present.HasValue || present.Value == 0;
        var missingZero = !missing.HasValue || missing.Value == 0;
        var badZero = !badQuality.HasValue || badQuality.Value == 0;

        return presentZero && missingZero && badZero;
    }

    private static bool IsNormalChunkStatus(string status) =>
        string.Equals(status, "ok", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(status, "ok_existing", StringComparison.OrdinalIgnoreCase) ||
        IsNoDataChunkStatus(status);

    private static bool IsNoDataChunkStatus(string status) =>
        string.Equals(status, "no_data", StringComparison.OrdinalIgnoreCase);
    private static bool IsSuccessfulFinal(string status) =>
        string.Equals(status, "done", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(status, "no_data", StringComparison.OrdinalIgnoreCase);

    private static bool IsPmuWithoutData(PmuAggregate pmu) =>
        pmu.Expected > 0 &&
        pmu.Present == 0 &&
        pmu.Missing == pmu.Expected &&
        pmu.BadQuality == 0 &&
        !pmu.HasTechnicalIssue;

    private void EnsurePmuOrderNoLock(string pmu)
    {
        if (string.IsNullOrWhiteSpace(pmu))
            return;

        if (_pmuOrder.ContainsKey(pmu))
            return;

        _pmuOrder[pmu] = ++_nextPmuOrder;
    }

    private long GetPmuOrderNoLock(string pmu)
    {
        // Em condições normais toda PMU já passou por StartPmu/CompleteChunk/
        // CompletePmu e possui ordem. O fallback evita reordenação imprevisível
        // caso algum nome apareça por um fluxo legado.
        return _pmuOrder.TryGetValue(pmu, out var order)
            ? order
            : long.MaxValue;
    }

    private static string FormatMetricWithPercent(
        long value,
        long denominator,
        bool complete,
        string denominatorLabel)
    {
        var text = FormatNumber(value);

        if (denominator > 0)
        {
            var pct = 100.0 * value / denominator;
            text += $" ({pct.ToString("F3", PtBr)}% {denominatorLabel})";
        }
        else if (value == 0)
        {
            text += " (0,000%)";
        }

        if (!complete)
            text += " [parcial]";

        return text;
    }

    private static string FormatNumber(long value) =>
        value.ToString("N0", PtBr);

    private static string FormatDuration(TimeSpan value)
    {
        if (value.TotalMinutes >= 1)
        {
            return $"{(int)value.TotalMinutes}m{value.Seconds:D2},{value.Milliseconds:D3}s";
        }

        return value.TotalSeconds.ToString("F3", PtBr) + "s";
    }

    private static TimeSpan GetElapsedSince(long startTick)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startTick;
        return TimeSpan.FromSeconds(elapsedTicks / (double)Stopwatch.Frequency);
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