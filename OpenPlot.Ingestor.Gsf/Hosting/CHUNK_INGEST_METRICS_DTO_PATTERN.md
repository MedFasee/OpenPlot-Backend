# Estrutura Hierárquica de Progresso de Ingestão - Job + PMUs + Chunks

## Visão Geral

O sistema de relatório de progresso de ingestão segue uma **hierarquia de três níveis** para evitar duplicação de informações gerais e otimizar o tamanho dos payloads JSON:

1. **JobIngestProgressDto** (nível superior): Contexto geral do job
2. **PmuIngestProgressDto** (nível intermediário): Agrupamento por PMU com métricas agregadas
3. **SimpleChunkIngestMetricsDto** (nível granular): Detalhes específicos de cada chunk

Esta estrutura responde à necessidade de o frontend sempre receber **informações atualizadas sobre toda a busca (search), para N PMUs, sem repetir campos gerais**.

## Hierarquia de DTOs

### 1. JobIngestProgressDto - Contexto Geral do Job

Contém informações do job que são emitidas **uma única vez** no snapshot:

```csharp
public sealed class JobIngestProgressDto
{
	// Identificação do Job
	public string JobId { get; set; }              // UUID do job (formato GUID string)
	public string JobSource { get; set; }          // Nome da fonte/PDC (ex: "UFSC_215_PMU")

	// Período e configuração do job
	public DateTime JobFromUtc { get; set; }       // Início do período (ISO 8601, UTC)
	public DateTime JobToUtc { get; set; }         // Fim do período (ISO 8601, UTC)
	public int SelectRate { get; set; }            // Taxa de amostragem (30, 60, 120 Hz)

	// Estado global do job
	public int TotalChunks { get; set; }           // Total de chunks do job
	public int ChunksCompleted { get; set; }       // Chunks já processados
	public int ProgressPercent { get; set; }       // Percentual de progresso (0-100)
	public string Status { get; set; }             // Status: "running", "done", "failed", etc
	public DateTime Timestamp { get; set; }        // Timestamp do snapshot (UTC)

	// Agregação de PMUs
	public List<PmuIngestProgressDto> Pmus { get; set; }  // Array de PMUs processadas
}
```

**Exemplo JSON:**
```json
{
  "job_id": "a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4g5h6i",
  "job_source": "UFSC_215_PMU",
  "job_from_utc": "2024-01-15T10:00:00Z",
  "job_to_utc": "2024-01-15T12:00:00Z",
  "select_rate": 60,
  "total_chunks": 12,
  "chunks_completed": 8,
  "progress_percent": 66,
  "status": "running",
  "timestamp": "2024-01-15T10:35:42.123Z",
  "pmus": [ /* veja abaixo */ ]
}
```

### 2. PmuIngestProgressDto - Contexto de PMU (Agrupamento)

Agrupa todos os chunks de uma PMU específica:

```csharp
public sealed class PmuIngestProgressDto
{
	public string PmuId { get; set; }                       // ID da PMU
	public string TerminalId { get; set; }                  // ID do terminal

	public int ChunksCompleted { get; set; }                // Chunks desta PMU processados

	// Métricas agregadas desta PMU
	public double TotalProcessingTimeMs { get; set; }       // Tempo total de processamento
	public int TotalFramesExpected { get; set; }            // Total de frames esperados
	public int? TotalFramesPresent { get; set; }            // Total de frames presentes

	public string Status { get; set; }                      // Status da PMU

	// Array de chunks (sem contexto duplicado)
	public List<SimpleChunkIngestMetricsDto> Chunks { get; set; }
}
```

**Exemplo JSON:**
```json
{
  "pmu_id": "PMU_UFSC_215",
  "terminal_id": "TERM_UFSC",
  "chunks_completed": 4,
  "total_processing_time_milliseconds": 5200.50,
  "total_frames_expected": 2400,
  "total_frames_present": 2390,
  "status": "running",
  "chunks": [ /* veja abaixo */ ]
}
```

### 3. SimpleChunkIngestMetricsDto - Detalhes por Chunk

Formato simplificado de chunks (sem contexto duplicado):

```csharp
public sealed class SimpleChunkIngestMetricsDto
{
	public int ChunkIndex { get; set; }                  // Posição do chunk na sequência
	public DateTime FromUtc { get; set; }                // Início deste chunk
	public DateTime ToUtc { get; set; }                  // Fim deste chunk

	public double ProcessingTimeMilliseconds { get; set; } // Tempo de processamento

	public int ExpectedFrames { get; set; }              // Frames esperados neste intervalo
	public int? PresentFrames { get; set; }              // Frames presentes
	public int? MissingFrames { get; set; }              // Frames faltantes
	public int? BadQualityFrames { get; set; }           // Frames com qualidade ruim

	public string Status { get; set; }                   // Status: "ok", "no_data", "failed", etc
}
```

**Exemplo JSON:**
```json
{
  "chunk_index": 2,
  "from_utc": "2024-01-15T10:10:00Z",
  "to_utc": "2024-01-15T10:20:00Z",
  "processing_time_milliseconds": 1250.50,
  "expected_frames": 600,
  "present_frames": 598,
  "missing_frames": 2,
  "bad_quality_frames": 1,
  "status": "ok"
}
```

## Snapshot Completo Exemplo

```json
{
  "job_id": "a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4g5h6i",
  "job_source": "UFSC_215_PMU",
  "job_from_utc": "2024-01-15T10:00:00Z",
  "job_to_utc": "2024-01-15T12:00:00Z",
  "select_rate": 60,
  "total_chunks": 12,
  "chunks_completed": 8,
  "progress_percent": 66,
  "status": "running",
  "timestamp": "2024-01-15T10:35:42.123Z",
  "pmus": [
	{
	  "pmu_id": "PMU_UFSC_215",
	  "terminal_id": "TERM_UFSC",
	  "chunks_completed": 4,
	  "total_processing_time_milliseconds": 5200.50,
	  "total_frames_expected": 2400,
	  "total_frames_present": 2390,
	  "status": "running",
	  "chunks": [
		{
		  "chunk_index": 0,
		  "from_utc": "2024-01-15T10:00:00Z",
		  "to_utc": "2024-01-15T10:10:00Z",
		  "processing_time_milliseconds": 1200.00,
		  "expected_frames": 600,
		  "present_frames": 595,
		  "missing_frames": 5,
		  "bad_quality_frames": 0,
		  "status": "ok"
		},
		{
		  "chunk_index": 1,
		  "from_utc": "2024-01-15T10:10:00Z",
		  "to_utc": "2024-01-15T10:20:00Z",
		  "processing_time_milliseconds": 1250.50,
		  "expected_frames": 600,
		  "present_frames": 598,
		  "missing_frames": 2,
		  "bad_quality_frames": 1,
		  "status": "ok"
		}
	  ]
	},
	{
	  "pmu_id": "PMU_UFSC_216",
	  "terminal_id": "TERM_UFSC",
	  "chunks_completed": 4,
	  "total_processing_time_milliseconds": 4800.25,
	  "total_frames_expected": 2400,
	  "total_frames_present": 2400,
	  "status": "running",
	  "chunks": [
		{
		  "chunk_index": 0,
		  "from_utc": "2024-01-15T10:00:00Z",
		  "to_utc": "2024-01-15T10:10:00Z",
		  "processing_time_milliseconds": 1150.00,
		  "expected_frames": 600,
		  "present_frames": 600,
		  "missing_frames": 0,
		  "bad_quality_frames": 0,
		  "status": "ok"
		}
	  ]
	}
  ]
}
```

## Benefícios da Hierarquia

| Aspecto | Benefício |
|---------|-----------|
| **Sem Repetição** | Contexto de job (source, período, select_rate) emitido uma única vez |
| **Redução de Payload** | Chunks simplificados: sem job_id, job_source, job_from_utc, etc repetidos |
| **Agrupamento Natural** | Chunks agrupados por PMU → Fácil para frontend processar por terminal |
| **Métricas Agregadas** | Totais por PMU facilitam dashboard e visualizações |
| **Escalabilidade** | Estrutura permanece consistente independentemente de N PMUs |

## Método de Geração

O snapshot hierárquico é gerado pelo método `IngestorProgressReporter.GetProgressSnapshot()`:

```csharp
public JobIngestProgressDto GetProgressSnapshot(
	Guid jobId,
	string jobSource,
	DateTime jobFromUtc,
	DateTime jobToUtc,
	int selectRate)
{
	// Retorna estrutura completa com hierarquia
	// Job → PMUs agrupadas → Chunks simplificados
}
```

## Integração com IngestorProgressReporter

**ChunkIngestMetricsDto** ainda existe para casos onde contexto isolado é necessário.

**SimpleChunkIngestMetricsDto** é derivado de `ChunkIngestMetrics` mediante:
```csharp
var simpleChunk = SimpleChunkIngestMetricsDto.FromMetrics(metrics);
```

## Localização dos Tipos

- **Arquivo**: `OpenPlot.Ingestor.Gsf/Hosting/IngestorMetrics.cs`
- **Tipos**: 
  - `JobIngestProgressDto`
  - `PmuIngestProgressDto`
  - `SimpleChunkIngestMetricsDto`
  - `ChunkIngestMetricsDto` (formato detalhado, para compatibilidade)

## Convenção de Nomenclatura

Todas as propriedades JSON utilizam **snake_case** via atributo `JsonPropertyName`.

## Status Possíveis de Chunk

- **ok**: Processamento completo e dados inseridos
- **no_data**: Nenhuma amostra no historiador
- **skipped_existing**: Chunk já existente no banco
- **failed**: Erro no processamento
- **canceled**: Job cancelado
- **bad_connection**: Erro de conexão com banco
