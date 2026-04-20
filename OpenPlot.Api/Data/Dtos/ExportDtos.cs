using System.Text.Json.Serialization;

namespace OpenPlot.Data.Dtos
{
    public sealed class QueueExportRequest
    {
        [JsonPropertyName("run_id")]
        public string? run_id { get; set; }

        [JsonPropertyName("runId")]
        public string? runId { get; set; }

        public string? format { get; set; }

        public string? ResolveRunId() =>
            !string.IsNullOrWhiteSpace(run_id) ? run_id : runId;
    }

    public sealed class ExportRunStatusRow
    {
        public Guid run_id { get; set; }
        public string format { get; set; } = "";
        public string status { get; set; } = "";
        public int progress { get; set; }
        public string? message { get; set; }
        public string? error { get; set; }
        public string? dir_path { get; set; }
        public string? file_name { get; set; }
        public long? size_bytes { get; set; }
        public string? sha256 { get; set; }
        public DateTime created_at { get; set; }
        public DateTime? started_at { get; set; }
        public DateTime? finished_at { get; set; }
    }

    internal sealed class ExpiredExportFileRow
    {
        public string? dir_path { get; set; }
        public string? file_name { get; set; }
    }
}
