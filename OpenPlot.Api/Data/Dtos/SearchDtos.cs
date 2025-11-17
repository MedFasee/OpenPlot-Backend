using Microsoft.AspNetCore.Mvc;

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
        public string Source { get; set; } = "";     // = OpenPlot.pdc.name
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
    /*
    public record ByRunQuery(
        [FromQuery(Name = "run_id")] Guid RunId,
        [FromQuery(Name = "phase")] string Phase,
        [FromQuery(Name = "unit")] string Unit = "raw",
        [FromQuery(Name = "max_points")] int MaxPoints = 5000
    );
    */

    public record VoltRow(
    int Signal_Id, int Pdc_Pmu_Id, string Phase, string Component,
    string Id_Name, string Pdc_Name, int? Volt_Level,
    DateTime Ts, double Value
);

    public class SeqPosRow
    {
        public int Signal_Id { get; init; }
        public int Pdc_Pmu_Id { get; init; }
        public string Phase { get; init; } = "";
        public string Component { get; init; } = "";
        public string Id_Name { get; init; } = "";
        public string Pdc_Name { get; init; } = "";
        public double? Volt_Level { get; init; }
        public DateTime Ts { get; init; }
        public double Value { get; init; }
    }


    public sealed class ByRunQuery
    {
        [FromQuery(Name = "run_Id")]
        public Guid RunId { get; set; }

        // Fase A/B/C – obrigatória quando tri = false
        public string? Phase { get; init; }

        [FromQuery(Name = "max_points")]
        public int MaxPoints { get; init; } = 2000;

        // "raw" ou "pu"
        public string? Unit { get; init; }

        // Se true → plota trifásico (A,B,C) da PMU indicada em Pmu
        public bool Tri { get; init; } = false;

        // id_name da PMU (ex.: "N_PA_Belem_UFPA").
        // Obrigatório quando Tri = true.
        [FromQuery(Name = "id_name")]
        public string? Pmu { get; init; }
    }

}
