namespace OpenPlot.Data.Dtos
{
    public sealed class SearchRunRow
    {
        public Guid id { get; set; }
        public string source { get; set; } = "";
        public string? terminal_id { get; set; }
        public DateTime from_ts { get; set; }
        public DateTime to_ts { get; set; }
        public int select_rate { get; set; }
        public string status { get; set; } = "";
        public DateTime created_at { get; set; }
    }

    public sealed class SearchRunFull
    {
        public Guid id { get; set; }
        public string source { get; set; } = "";
        public string? terminal_id { get; set; }
        public string signals { get; set; } = ""; // jsonb::text
        public DateTime from_ts { get; set; }
        public DateTime to_ts { get; set; }
        public int select_rate { get; set; }
        public string status { get; set; } = "";
        public DateTime created_at { get; set; }
    }

    public sealed class PmuMeta
    {
        public int pmu_id { get; set; }
        public string id_name { get; set; } = "";
        public string? full_name { get; set; }
        public int? volt_level { get; set; }
        public string? area { get; set; }
        public string? state { get; set; }
        public string? station { get; set; }
    }

    public sealed class SearchReq
    {
        public string Source { get; set; } = "";     // = medi.pdc.name
        public List<string> Pmus { get; set; } = new();
        public DateTime From { get; set; }
        public DateTime To { get; set; }
        public string? Resolution { get; set; }
    }
    public record SearchRequest(
    string Source,
    string TerminalId,              // ⬅️ novo
    string[] Signals,
    DateTime From,
    DateTime To,
    string Resolution
);
}
