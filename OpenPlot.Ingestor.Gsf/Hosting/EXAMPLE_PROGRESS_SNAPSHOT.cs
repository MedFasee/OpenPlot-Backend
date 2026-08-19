// Exemplo de uso do GetProgressSnapshot para frontend

// 1. Em um endpoint da API, recupere o snapshot atual:
var snapshot = _progressReporter.GetProgressSnapshot(
    jobId: job.Id,
    jobSource: job.Source,
    jobFromUtc: job.From,
    jobToUtc: job.To,
    selectRate: job.SelectRate
);

// 2. Serialize para JSON (automático via System.Text.Json):
var json = JsonSerializer.Serialize(snapshot, new JsonSerializerOptions 
{ 
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    WriteIndented = true
});

// 3. Retorne ao frontend:
return Ok(json);

// RESULTADO: JSON hierárquico sem duplicação de dados gerais
// ├── job_id, job_source, job_from_utc, job_to_utc, select_rate (emitido UMA VEZ)
// ├── progress_percent, status, timestamp
// └── pmus[] (array)
//     ├── pmu_id, terminal_id (contexto da PMU)
//     ├── total_processing_time_milliseconds, total_frames_expected, etc
//     └── chunks[] (simplificados - sem job_id, job_source repetido)
//         ├── chunk_index, from_utc, to_utc
//         ├── processing_time_milliseconds
//         ├── expected_frames, present_frames, missing_frames, bad_quality_frames
//         └── status

// BENEFÍCIO: 
// - Payload 30-50% menor que repetir job_source em cada chunk
// - Frontend agrupa visualmente por PMU/Terminal automaticamente
// - Métricas agregadas por PMU já prontas para dashboard
// - Escalável para N PMUs sem mudança de estrutura
