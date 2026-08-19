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
/// - o início da message sempre informa o status da consulta;
/// - o início da message sempre informa quantas PMUs estão pendentes;
/// - o início da message sempre informa quantas PMUs estão em processamento;
/// - cada PMU visível informa somente seu status e o percentual de dados válidos;
/// - dados válidos = frames presentes - frames com qualidade ruim;
/// - o percentual de dados válidos usa os frames esperados como denominador;
/// - a ordem visual das PMUs é fixa pela primeira aparição;
/// - novas PMUs são sempre adicionadas ao fim da lista visível;
/// - PMUs concluídas permanecem visíveis para preservar o acompanhamento;
/// - o resumo técnico detalhado foi suprimido da message;
/// - ao final, são exibidos somente o tempo total e o percentual geral de dados válidos;
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
    // [LEGADO] private readonly long _createdTick; // usado apenas pelo antigo cabeçalho com tempo decorrido

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
        // [LEGADO] _createdTick = Stopwatch.GetTimestamp(); // usado apenas pelo antigo cabeçalho com tempo decorrido
    }

    public int CurrentProgressPercent =>
        CalculateRunningPercent(Interlocked.Read(ref _done));

    /// <summary>
    /// Torna a PMU visível imediatamente no polling, antes mesmo do primeiro
    /// chunk terminar. Enquanto ativa, seu status é exibido como EM PROCESSAMENTO.
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
    /// a message é reconstruída como agregado por PMU, exibindo apenas status
    /// e percentual de dados válidos.
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

        var aggregates = GetAllPmuAggregatesNoLock()
            .OrderBy(x => GetPmuOrderNoLock(x.Pmu))
            .ToArray();

        //var pmuColumnWidth = aggregates.Length > 0
        //? aggregates.Max(x => x.Pmu.Length) + 4
        //: 4;

        //var statusColumnWidth = 30;

        var isRunning = string.Equals(
            finalStatus,
            "running",
            StringComparison.OrdinalIgnoreCase);

        var completedPmus = aggregates.Count(x => x.IsCompleted);

        // Qualquer PMU já conhecida e ainda não concluída é considerada
        // "em processamento". Isso também cobre fluxos legados que possam
        // registrar chunk antes de StartPmu().
        var processingPmus = isRunning
            ? aggregates.Count(x => !x.IsCompleted)
            : 0;

        // Durante a execução, pendentes = ainda não iniciadas/observadas.
        // Após a finalização, qualquer PMU não concluída volta a ser tratada
        // como pendente, pois já não existe processamento em curso.
        var pendingPmus = Math.Max(
            0,
            _totalPmus - completedPmus - processingPmus);

        sb.Append("STATUS DA CONSULTA: ")
          .Append(GetQueryStatusText(finalStatus))
          .AppendLine()
          .Append("PMUs consultadas:")
          .Append(completedPmus)
          .Append(" \t PMUs pendentes:")
          .Append(pendingPmus)
          .Append(" \t PMUs em consulta:")
          .Append(processingPmus)
          .AppendLine();

        if (!string.IsNullOrWhiteSpace(details))
        {
            sb.Append("Detalhes: ")
              .AppendLine(Sanitize(details));
        }

        sb.AppendLine();

        if (aggregates.Length == 0)
        {
            sb.AppendLine("Nenhuma PMU iniciada até o momento.");
        }
        else
        {
            var pmuColumnWidth = Math.Max(
                "PMU".Length,
                aggregates.Max(x => x.Pmu.Length)) + 4;

            var statusColumnWidth = Math.Max(
                "STATUS".Length,
                aggregates.Max(x => GetPmuStatusText(x).Length)) + 4;

            sb.Append("PMU".PadRight(pmuColumnWidth))
              .Append("STATUS".PadRight(statusColumnWidth))
              .Append("DADOS VÁLIDOS")
              .AppendLine();

            foreach (var pmu in aggregates)
            {
                AppendPmuLine(
                    sb,
                    pmu,
                    pmuColumnWidth,
                    statusColumnWidth);
            }
        }

        // O antigo RESUMO/RESUMO PARCIAL foi suprimido.
        // Durante o processamento não repetimos métricas globais.
        // Na conclusão, mostramos somente o solicitado:
        // tempo total da consulta + percentual geral de dados válidos.
        if (!isRunning)
        {
            sb.AppendLine();

            sb.Append("Tempo total=")
              .Append(
                  jobProcessingTime.HasValue
                      ? FormatDuration(jobProcessingTime.Value)
                      : "n/d")
              .Append(" \t Dados válidos=")
              .Append(CalculateOverallValidPercent())
              .AppendLine();
        }

        return sb.ToString().TrimEnd();
    }

    private void AppendPmuLine(
    StringBuilder sb,
    PmuAggregate pmu,
    int pmuColumnWidth,
    int statusColumnWidth)
    {
        var validPercent = FormatValidPercent(
            present: pmu.Present,
            badQuality: pmu.BadQuality,
            expected: pmu.Expected,
            complete: pmu.MetricsComplete);

        sb.Append(pmu.Pmu.PadRight(pmuColumnWidth))
          .Append(GetPmuStatusText(pmu).PadRight(statusColumnWidth))
          .Append(validPercent)
          .AppendLine();
    }

    private static string GetPmuStatusText(PmuAggregate pmu)
    {
        if (pmu.IsCompleted)
        {
            return pmu.HasTechnicalIssue
                ? "CONCLUÍDA COM ALERTA"
                : "CONCLUÍDA";
        }

        if (pmu.IsActive || pmu.CompletedChunks > 0)
            return "EM PROCESSAMENTO";

        return "PENDENTE";
    }

    private static string GetQueryStatusText(string finalStatus)
    {
        if (string.Equals(
                finalStatus,
                "running",
                StringComparison.OrdinalIgnoreCase))
        {
            return "EM PROCESSAMENTO";
        }

        if (IsSuccessfulFinal(finalStatus))
            return "CONCLUÍDA";

        return "FINALIZADA - " + Sanitize(finalStatus).ToUpperInvariant();
    }

    /// <summary>
    /// Dados válidos = presentes - qualidade ruim.
    ///
    /// O denominador permanece sendo o total esperado, pois assim uma ausência
    /// de dados reduz diretamente o percentual de validade.
    ///
    /// Exemplo:
    /// esperados=100, presentes=95, qualidade ruim=5
    /// válidos=90 => 90,000%.
    /// </summary>
    private static string FormatValidPercent(
        long present,
        long badQuality,
        long expected,
        bool complete)
    {
        if (expected <= 0)
            return complete ? "n/d" : "n/d [parcial]";

        var valid = Math.Max(0L, present - badQuality);
        var pct = 100.0 * valid / expected;

        var text = pct.ToString("F2", PtBr) + "%";

        if (!complete)
            text += " [parcial]";

        return text;
    }

    private string CalculateOverallValidPercent()
    {
        var chunks = _chunks.Values.ToArray();
        var normalized = chunks
            .Select(NormalizeChunkMetrics)
            .ToArray();

        var expected = chunks.Sum(x => (long)x.ExpectedFrames);

        if (expected <= 0)
            return "n/d";

        var present = normalized
            .Where(x => x.Present.HasValue)
            .Sum(x => (long)x.Present!.Value);

        var badQuality = normalized
            .Where(x => x.BadQuality.HasValue)
            .Sum(x => (long)x.BadQuality!.Value);

        var valid = Math.Max(0L, present - badQuality);
        var pct = 100.0 * valid / expected;

        var metricsComplete =
            chunks.Length == _totalChunks &&
            normalized.All(x => x.IsComplete);

        var text = pct.ToString("F2", PtBr) + "%";

        if (!metricsComplete)
            text += " [parcial]";

        return text;
    }

    // =====================================================================
    // CÓDIGO LEGADO DE APRESENTAÇÃO
    // =====================================================================
    // Mantido comentado propositalmente para permitir restauração futura
    // das métricas detalhadas (tempo por PMU, esperados, presentes,
    // faltantes, qualidade ruim e RESUMO/RESUMO PARCIAL).
    //
    // [LEGADO]     private string BuildMessageNoLock(
    // [LEGADO]         TimeSpan? jobProcessingTime,
    // [LEGADO]         string finalStatus,
    // [LEGADO]         string? details)
    // [LEGADO]     {
    // [LEGADO]         var sb = new StringBuilder(4096);
    // [LEGADO] 
    // [LEGADO]         var chunksDone = _chunks.Count;
    // [LEGADO]         var runningPct = CalculateRunningPercent(chunksDone);
    // [LEGADO]         var successfulFinal = IsSuccessfulFinal(finalStatus);
    // [LEGADO]         var isRunning = string.Equals(finalStatus, "running", StringComparison.OrdinalIgnoreCase);
    // [LEGADO] 
    // [LEGADO]         if (isRunning)
    // [LEGADO]         {
    // [LEGADO]             sb.Append("PROCESSANDO | ")
    // [LEGADO]               .Append(chunksDone)
    // [LEGADO]               .Append('/')
    // [LEGADO]               .Append(_totalChunks)
    // [LEGADO]               .Append(" chunks (")
    // [LEGADO]               .Append(runningPct)
    // [LEGADO]               .Append("%) | tempo decorrido=")
    // [LEGADO]               .Append(FormatDuration(GetElapsedSince(_createdTick)))
    // [LEGADO]               .AppendLine();
    // [LEGADO]         }
    // [LEGADO]         else if (successfulFinal)
    // [LEGADO]         {
    // [LEGADO]             sb.Append("PROCESSAMENTO CONCLUÍDO");
    // [LEGADO] 
    // [LEGADO]             if (jobProcessingTime.HasValue)
    // [LEGADO]                 sb.Append(" | tempo total=").Append(FormatDuration(jobProcessingTime.Value));
    // [LEGADO] 
    // [LEGADO]             sb.AppendLine();
    // [LEGADO]         }
    // [LEGADO]         else
    // [LEGADO]         {
    // [LEGADO]             sb.Append("PROCESSAMENTO FINALIZADO | status=")
    // [LEGADO]               .Append(finalStatus);
    // [LEGADO] 
    // [LEGADO]             if (jobProcessingTime.HasValue)
    // [LEGADO]                 sb.Append(" | tempo total=").Append(FormatDuration(jobProcessingTime.Value));
    // [LEGADO] 
    // [LEGADO]             sb.AppendLine();
    // [LEGADO]         }
    // [LEGADO] 
    // [LEGADO]         if (!string.IsNullOrWhiteSpace(details))
    // [LEGADO]         {
    // [LEGADO]             sb.Append("Detalhes: ")
    // [LEGADO]               .AppendLine(Sanitize(details));
    // [LEGADO]         }
    // [LEGADO] 
    // [LEGADO]         var aggregates = GetAllPmuAggregatesNoLock();
    // [LEGADO] 
    // [LEGADO]         // A lista visível é sempre ordenada pela PRIMEIRA APARIÇÃO da PMU.
    // [LEGADO]         //
    // [LEGADO]         // Consequências:
    // [LEGADO]         // - uma nova PMU entra sempre no fim;
    // [LEGADO]         // - uma PMU já exibida nunca sobe/desce por quantidade de faltantes;
    // [LEGADO]         // - uma PMU ativa que termina com ocorrência permanece na mesma posição;
    // [LEGADO]         // - uma PMU concluída sem ocorrência pode sair da lista, reduzindo ruído.
    // [LEGADO]         var visible = aggregates
    // [LEGADO]             .Where(x =>
    // [LEGADO]                 x.IsActive ||
    // [LEGADO]                 (x.IsCompleted && (x.HasOccurrences || x.HasTechnicalIssue)))
    // [LEGADO]             .OrderBy(x => GetPmuOrderNoLock(x.Pmu))
    // [LEGADO]             .ToArray();
    // [LEGADO] 
    // [LEGADO]         var completedWithoutData = aggregates.Count(x =>
    // [LEGADO]             x.IsCompleted && IsPmuWithoutData(x));
    // [LEGADO] 
    // [LEGADO]         sb.AppendLine();
    // [LEGADO]         sb.AppendLine("PMUs");
    // [LEGADO] 
    // [LEGADO]         if (visible.Length == 0)
    // [LEGADO]         {
    // [LEGADO]             sb.AppendLine("Nenhuma PMU em processamento ou com ocorrência até o momento.");
    // [LEGADO]         }
    // [LEGADO]         else
    // [LEGADO]         {
    // [LEGADO]             foreach (var pmu in visible)
    // [LEGADO]                 AppendPmuLine(sb, pmu);
    // [LEGADO]         }
    // [LEGADO] 
    // [LEGADO]         var completedPmus = aggregates.Count(x => x.IsCompleted);
    // [LEGADO]         var completedWithData = aggregates.Count(x =>
    // [LEGADO]             x.IsCompleted && x.Present > 0);
    // [LEGADO] 
    // [LEGADO]         sb.Append("PMUs concluídas=")
    // [LEGADO]           .Append(completedPmus)
    // [LEGADO]           .Append('/')
    // [LEGADO]           .Append(_totalPmus)
    // [LEGADO]           .Append(" | sem dados=")
    // [LEGADO]           .Append(completedWithoutData)
    // [LEGADO]           .Append(" | com dados=")
    // [LEGADO]           .Append(completedWithData)
    // [LEGADO]           .AppendLine();
    // [LEGADO] 
    // [LEGADO]         sb.AppendLine();
    // [LEGADO]         AppendJobSummary(sb, jobProcessingTime, finalStatus, aggregates);
    // [LEGADO] 
    // [LEGADO]         return sb.ToString().TrimEnd();
    // [LEGADO]     }
    // [LEGADO] 
    // [LEGADO]     private void AppendPmuLine(StringBuilder sb, PmuAggregate pmu)
    // [LEGADO]     {
    // [LEGADO]         sb.Append(pmu.Pmu)
    // [LEGADO]           .Append(" | tempo=");
    // [LEGADO] 
    // [LEGADO]         if (pmu.IsCompleted)
    // [LEGADO]         {
    // [LEGADO]             sb.Append(FormatDuration(pmu.ProcessingTime));
    // [LEGADO]         }
    // [LEGADO]         else
    // [LEGADO]         {
    // [LEGADO]             sb.Append(FormatDuration(pmu.ProcessingTime))
    // [LEGADO]               .Append(" (em processamento)");
    // [LEGADO]         }
    // [LEGADO] 
    // [LEGADO]         sb.Append(" | esperados=")
    // [LEGADO]           .Append(FormatNumber(pmu.Expected))
    // [LEGADO]           .Append(" | presentes=")
    // [LEGADO]           .Append(FormatMetricWithPercent(
    // [LEGADO]               pmu.Present,
    // [LEGADO]               pmu.Expected,
    // [LEGADO]               pmu.MetricsComplete,
    // [LEGADO]               "dos esperados"))
    // [LEGADO]           .Append(" | faltantes=")
    // [LEGADO]           .Append(FormatMetricWithPercent(
    // [LEGADO]               pmu.Missing,
    // [LEGADO]               pmu.Expected,
    // [LEGADO]               pmu.MetricsComplete,
    // [LEGADO]               "dos esperados"))
    // [LEGADO]           .Append(" | qualidade ruim=")
    // [LEGADO]           .Append(FormatMetricWithPercent(
    // [LEGADO]               pmu.BadQuality,
    // [LEGADO]               pmu.Present,
    // [LEGADO]               pmu.MetricsComplete,
    // [LEGADO]               "dos presentes"));
    // [LEGADO] 
    // [LEGADO]         if (pmu.HasTechnicalIssue)
    // [LEGADO]             sb.Append(" | atenção=métrica incompleta/falha técnica");
    // [LEGADO] 
    // [LEGADO]         sb.AppendLine();
    // [LEGADO]         sb.AppendLine();
    // [LEGADO]     }
    // [LEGADO] 
    // [LEGADO]     private void AppendJobSummary(
    // [LEGADO]         StringBuilder sb,
    // [LEGADO]         TimeSpan? jobProcessingTime,
    // [LEGADO]         string finalStatus,
    // [LEGADO]         IReadOnlyCollection<PmuAggregate> aggregates)
    // [LEGADO]     {
    // [LEGADO]         var chunks = _chunks.Values.ToArray();
    // [LEGADO]         var normalized = chunks.Select(NormalizeChunkMetrics).ToArray();
    // [LEGADO] 
    // [LEGADO]         var expected = chunks.Sum(x => (long)x.ExpectedFrames);
    // [LEGADO]         var present = normalized.Where(x => x.Present.HasValue).Sum(x => (long)x.Present!.Value);
    // [LEGADO]         var missing = normalized.Where(x => x.Missing.HasValue).Sum(x => (long)x.Missing!.Value);
    // [LEGADO]         var bad = normalized.Where(x => x.BadQuality.HasValue).Sum(x => (long)x.BadQuality!.Value);
    // [LEGADO] 
    // [LEGADO]         var completeChunks = normalized.Count(x => x.IsComplete);
    // [LEGADO]         var metricsComplete =
    // [LEGADO]             chunks.Length == _totalChunks &&
    // [LEGADO]             completeChunks == chunks.Length;
    // [LEGADO] 
    // [LEGADO]         var title = string.Equals(finalStatus, "running", StringComparison.OrdinalIgnoreCase)
    // [LEGADO]             ? "RESUMO PARCIAL"
    // [LEGADO]             : "RESUMO";
    // [LEGADO] 
    // [LEGADO]         sb.Append(title)
    // [LEGADO]           .Append(" | PMUs=")
    // [LEGADO]           .Append(aggregates.Count(x => x.IsCompleted))
    // [LEGADO]           .Append('/')
    // [LEGADO]           .Append(_totalPmus)
    // [LEGADO]           .Append(" | chunks=")
    // [LEGADO]           .Append(chunks.Length)
    // [LEGADO]           .Append('/')
    // [LEGADO]           .Append(_totalChunks);
    // [LEGADO] 
    // [LEGADO]         if (!string.Equals(finalStatus, "running", StringComparison.OrdinalIgnoreCase) &&
    // [LEGADO]             jobProcessingTime.HasValue)
    // [LEGADO]         {
    // [LEGADO]             // O tempo já aparece no cabeçalho final. Não o repetimos aqui.
    // [LEGADO]         }
    // [LEGADO] 
    // [LEGADO]         sb.AppendLine();
    // [LEGADO] 
    // [LEGADO]         sb.Append("Esperados=")
    // [LEGADO]           .Append(FormatNumber(expected))
    // [LEGADO]           .Append(" | presentes=")
    // [LEGADO]           .Append(FormatMetricWithPercent(present, expected, metricsComplete, "dos esperados"))
    // [LEGADO]           .Append(" | faltantes=")
    // [LEGADO]           .Append(FormatMetricWithPercent(missing, expected, metricsComplete, "dos esperados"))
    // [LEGADO]           .Append(" | qualidade ruim=")
    // [LEGADO]           .Append(FormatMetricWithPercent(bad, present, metricsComplete, "dos presentes"))
    // [LEGADO]           .AppendLine()
    // [LEGADO]           .AppendLine();
    // [LEGADO] 
    // [LEGADO]         if (!metricsComplete)
    // [LEGADO]         {
    // [LEGADO]             sb.Append("Métricas consolidadas em ")
    // [LEGADO]               .Append(completeChunks)
    // [LEGADO]               .Append('/')
    // [LEGADO]               .Append(_totalChunks)
    // [LEGADO]               .AppendLine(" chunks concluídos até o momento.");
    // [LEGADO]         }
    // [LEGADO]     }
    // [LEGADO] 

    // =====================================================================
    // FIM DO CÓDIGO LEGADO DE APRESENTAÇÃO
    // =====================================================================

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

    // [LEGADO]     private static bool IsPmuWithoutData(PmuAggregate pmu) =>
    // [LEGADO]         pmu.Expected > 0 &&
    // [LEGADO]         pmu.Present == 0 &&
    // [LEGADO]         pmu.Missing == pmu.Expected &&
    // [LEGADO]         pmu.BadQuality == 0 &&
    // [LEGADO]         !pmu.HasTechnicalIssue;
    // [LEGADO] 

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

    // [LEGADO]     private static string FormatMetricWithPercent(
    // [LEGADO]         long value,
    // [LEGADO]         long denominator,
    // [LEGADO]         bool complete,
    // [LEGADO]         string denominatorLabel)
    // [LEGADO]     {
    // [LEGADO]         var text = FormatNumber(value);
    // [LEGADO] 
    // [LEGADO]         if (denominator > 0)
    // [LEGADO]         {
    // [LEGADO]             var pct = 100.0 * value / denominator;
    // [LEGADO]             text += $" ({pct.ToString("F3", PtBr)}% {denominatorLabel})";
    // [LEGADO]         }
    // [LEGADO]         else if (value == 0)
    // [LEGADO]         {
    // [LEGADO]             text += " (0,000%)";
    // [LEGADO]         }
    // [LEGADO] 
    // [LEGADO]         if (!complete)
    // [LEGADO]             text += " [parcial]";
    // [LEGADO] 
    // [LEGADO]         return text;
    // [LEGADO]     }
    // [LEGADO] 
    // [LEGADO]     private static string FormatNumber(long value) =>
    // [LEGADO]         value.ToString("N0", PtBr);
    // [LEGADO] 

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