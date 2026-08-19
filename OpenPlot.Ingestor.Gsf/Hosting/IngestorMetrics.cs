using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace OpenPlot.Ingestor.Gsf.Hosting;

/// <summary>
/// Métricas de ingestão de um chunk de dados específico.
/// Um chunk é um intervalo de tempo dentro de um job de ingestão.
/// </summary>
internal sealed record ChunkIngestMetrics(
    Guid JobId,
    string JobSource,
    string Terminal,
    DateTime JobFromUtc,
    DateTime JobToUtc,
    int SelectRate,
    int ChunkIndex,
    string Pmu,
    DateTime FromUtc,
    DateTime ToUtc,
    TimeSpan ProcessingTime,
    int ExpectedFrames,
    int? PresentFrames,
    int? MissingFrames,
    int? BadQualityFrames,
    string Status)
{
    /// <summary>
    /// Chave única para identificar este chunk (usado como chave de dicionário).
    /// Formato: "{Pmu}_{ChunkIndex}"
    /// </summary>
    public string Key => $"{Pmu}_{ChunkIndex}";

    /// <summary>
    /// Detalhes em formato string para logging ou informação.
    /// </summary>
    public string Details => $"Pmu={Pmu}, ChunkIndex={ChunkIndex}, From={FromUtc:O}, To={ToUtc:O}, Status={Status}";
}

/// <summary>
/// DTO para serialização JSON de ChunkIngestMetrics, seguindo o padrão snake_case do projeto OpenPlot.
/// Expõe métricas de ingestão com contexto completo de job e chunk (formato detalhado).
/// Usado quando informações isoladas do chunk são necessárias.
/// </summary>
internal sealed class ChunkIngestMetricsDto
{
    // Contexto do Job (aqui para compatibilidade e contexto isolado)
    [JsonPropertyName("job_id")]
    public string JobId { get; set; } = string.Empty;

    [JsonPropertyName("job_source")]
    public string JobSource { get; set; } = string.Empty;

    [JsonPropertyName("job_from_utc")]
    public DateTime JobFromUtc { get; set; }

    [JsonPropertyName("job_to_utc")]
    public DateTime JobToUtc { get; set; }

    [JsonPropertyName("select_rate")]
    public int SelectRate { get; set; }

    // Contexto do Terminal/PMU
    [JsonPropertyName("terminal_id")]
    public string TerminalId { get; set; } = string.Empty;

    [JsonPropertyName("pmu")]
    public string Pmu { get; set; } = string.Empty;

    // Contexto do Chunk
    [JsonPropertyName("chunk_index")]
    public int ChunkIndex { get; set; }

    [JsonPropertyName("from_utc")]
    public DateTime FromUtc { get; set; }

    [JsonPropertyName("to_utc")]
    public DateTime ToUtc { get; set; }

    // Métricas do Chunk
    [JsonPropertyName("processing_time_milliseconds")]
    public double ProcessingTimeMilliseconds { get; set; }

    [JsonPropertyName("expected_frames")]
    public int ExpectedFrames { get; set; }

    [JsonPropertyName("present_frames")]
    public int? PresentFrames { get; set; }

    [JsonPropertyName("missing_frames")]
    public int? MissingFrames { get; set; }

    [JsonPropertyName("bad_quality_frames")]
    public int? BadQualityFrames { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    /// <summary>
    /// Converte um ChunkIngestMetrics para seu equivalente DTO.
    /// </summary>
    internal static ChunkIngestMetricsDto FromMetrics(ChunkIngestMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(metrics);

        return new ChunkIngestMetricsDto
        {
            JobId = metrics.JobId.ToString("D"),
            JobSource = metrics.JobSource,
            JobFromUtc = metrics.JobFromUtc,
            JobToUtc = metrics.JobToUtc,
            SelectRate = metrics.SelectRate,
            TerminalId = metrics.Terminal,
            Pmu = metrics.Pmu,
            ChunkIndex = metrics.ChunkIndex,
            FromUtc = metrics.FromUtc,
            ToUtc = metrics.ToUtc,
            ProcessingTimeMilliseconds = metrics.ProcessingTime.TotalMilliseconds,
            ExpectedFrames = metrics.ExpectedFrames,
            PresentFrames = metrics.PresentFrames,
            MissingFrames = metrics.MissingFrames,
            BadQualityFrames = metrics.BadQualityFrames,
            Status = metrics.Status
        };
    }

    /// <summary>
    /// Converte este DTO de volta para ChunkIngestMetrics.
    /// </summary>
    internal ChunkIngestMetrics ToMetrics()
    {
        return new ChunkIngestMetrics(
            JobId: Guid.Parse(JobId),
            JobSource: JobSource,
            Terminal: TerminalId,
            JobFromUtc: JobFromUtc,
            JobToUtc: JobToUtc,
            SelectRate: SelectRate,
            ChunkIndex: ChunkIndex,
            Pmu: Pmu,
            FromUtc: FromUtc,
            ToUtc: ToUtc,
            ProcessingTime: TimeSpan.FromMilliseconds(ProcessingTimeMilliseconds),
            ExpectedFrames: ExpectedFrames,
            PresentFrames: PresentFrames,
            MissingFrames: MissingFrames,
            BadQualityFrames: BadQualityFrames,
            Status: Status
        );
    }
}

/// <summary>
/// DTO simplificado para chunk dentro do contexto agregado de PMU.
/// Remove contexto de job e terminal (redundante), focando apenas em métricas do chunk.
/// Usado em PmuIngestProgressDto para reduzir tamanho JSON.
/// </summary>
internal sealed class SimpleChunkIngestMetricsDto
{
    [JsonPropertyName("chunk_index")]
    public int ChunkIndex { get; set; }

    [JsonPropertyName("from_utc")]
    public DateTime FromUtc { get; set; }

    [JsonPropertyName("to_utc")]
    public DateTime ToUtc { get; set; }

    [JsonPropertyName("processing_time_milliseconds")]
    public double ProcessingTimeMilliseconds { get; set; }

    [JsonPropertyName("expected_frames")]
    public int ExpectedFrames { get; set; }

    [JsonPropertyName("present_frames")]
    public int? PresentFrames { get; set; }

    [JsonPropertyName("missing_frames")]
    public int? MissingFrames { get; set; }

    [JsonPropertyName("bad_quality_frames")]
    public int? BadQualityFrames { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    /// <summary>
    /// Converte um ChunkIngestMetrics para versão simplificada (sem contexto duplicado).
    /// </summary>
    internal static SimpleChunkIngestMetricsDto FromMetrics(ChunkIngestMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(metrics);

        return new SimpleChunkIngestMetricsDto
        {
            ChunkIndex = metrics.ChunkIndex,
            FromUtc = metrics.FromUtc,
            ToUtc = metrics.ToUtc,
            ProcessingTimeMilliseconds = metrics.ProcessingTime.TotalMilliseconds,
            ExpectedFrames = metrics.ExpectedFrames,
            PresentFrames = metrics.PresentFrames,
            MissingFrames = metrics.MissingFrames,
            BadQualityFrames = metrics.BadQualityFrames,
            Status = metrics.Status
        };
    }
}

/// <summary>
/// DTO para uma PMU dentro de um job de ingestão.
/// Agrupa todos os chunks processados para uma PMU específica.
/// </summary>
internal sealed class PmuIngestProgressDto
{
    [JsonPropertyName("pmu_id")]
    public string PmuId { get; set; } = string.Empty;

    [JsonPropertyName("terminal_id")]
    public string TerminalId { get; set; } = string.Empty;

    [JsonPropertyName("chunks")]
    public List<SimpleChunkIngestMetricsDto> Chunks { get; set; } = new();

    [JsonPropertyName("total_processing_time_milliseconds")]
    public double TotalProcessingTimeMs { get; set; }

    [JsonPropertyName("chunks_completed")]
    public int ChunksCompleted { get; set; }

    [JsonPropertyName("total_frames_expected")]
    public int TotalFramesExpected { get; set; }

    [JsonPropertyName("total_frames_present")]
    public int? TotalFramesPresent { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = "running";
}

/// <summary>
/// DTO para o progresso completo de um job de ingestão.
/// Contém metadados do job + lista de PMUs com seus chunks.
/// Estrutura hierárquica evita duplicação de informações gerais.
/// </summary>
internal sealed class JobIngestProgressDto
{
    [JsonPropertyName("job_id")]
    public string JobId { get; set; } = string.Empty;

    [JsonPropertyName("job_source")]
    public string JobSource { get; set; } = string.Empty;

    [JsonPropertyName("job_from_utc")]
    public DateTime JobFromUtc { get; set; }

    [JsonPropertyName("job_to_utc")]
    public DateTime JobToUtc { get; set; }

    [JsonPropertyName("select_rate")]
    public int SelectRate { get; set; }

    [JsonPropertyName("pmus")]
    public List<PmuIngestProgressDto> Pmus { get; set; } = new();

    [JsonPropertyName("progress_percent")]
    public int ProgressPercent { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = "running";

    [JsonPropertyName("total_chunks")]
    public int TotalChunks { get; set; }

    [JsonPropertyName("chunks_completed")]
    public int ChunksCompleted { get; set; }

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
