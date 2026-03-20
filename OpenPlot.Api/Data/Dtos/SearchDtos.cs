using Microsoft.AspNetCore.Mvc;
using OpenPlot.Features.Runs.Handlers.Abstractions;

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
        public string status { get; set; } = default!;
        public DateTime created_at { get; set; }

        public bool shared { get; set; }
        public string username { get; set; } = default!;

        public bool owner { get; set; }
    }
    public sealed record SoftDeleteRun(Guid id, bool is_visible);

    public sealed record ShareRunRequest(
    Guid id,
    bool shared,
    string? label
);


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
        public string Source { get; set; } = "";   
        public List<string> Pmus { get; set; } = new();
        public DateTime From { get; set; }
        public DateTime To { get; set; }
        public string? Resolution { get; set; }
    }
    public record SearchRequest(
    string Source,
    string TerminalId, 
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
    public record FreqRow(
    int Signal_Id,
    int Pdc_Pmu_Id,
    string Id_Name,
    string Pdc_Name,
    DateTime Ts,
    double Value
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


    public sealed class ByRunQuery : ISeriesQuery
    {
        [FromQuery(Name = "run_Id")]
        public Guid RunId { get; init; }

        public bool Tri { get; init; }
        public string? Phase { get; init; }
        public string? Unit { get; init; }

        public string? Pmu { get; init; }

        [FromQuery(Name = "pmu")]
        public string[]? Pmus { get; init; }

        // string pra aceitar "all"
        [FromQuery(Name = "maxPoints")]
        public string? MaxPoints { get; init; }

        public bool MaxPointsIsAll =>
            string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

        public int ResolveMaxPoints(int @default = 5000)
        {
            if (MaxPointsIsAll) return int.MaxValue; // não será usado quando IsAll=true
            if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
            return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
        }
    }


    public sealed class PowerPlotQuery : ISeriesQuery
    {
        [FromQuery(Name = "run_id")] public Guid RunId { get; init; }

        [FromQuery(Name = "pmu")] public string[]? Pmu { get; init; }

        [FromQuery(Name = "which")] public string? Which { get; init; } // active|reactive

        [FromQuery(Name = "unit")] public string? Unit { get; init; }  // raw|mw

        [FromQuery(Name = "maxPoints")] public string? MaxPoints { get; init; }

        [FromQuery(Name = "tri")] public bool? Tri { get; init; }     // <- bool?

        [FromQuery(Name = "total")] public bool? Total { get; init; }   // <- bool?

        [FromQuery(Name = "phase")] public string? Phase { get; init; } // A|B|C

        public bool MaxPointsIsAll =>
            string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

        public int ResolveMaxPoints(int @default = 5000)
        {
            if (MaxPointsIsAll) return int.MaxValue;
            if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
            return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
        }
    }




        public sealed class PowerRow
        {
            public int Signal_Id { get; set; }
            public int Pdc_Pmu_Id { get; set; }
            public string? Phase { get; set; }        // A/B/C
            public string? Component { get; set; }    // MAG/ANG
            public string? Quantity { get; set; }     // voltage/current
            public string Id_Name { get; set; } = "";
            public string Pdc_Name { get; set; } = "";
            public DateTime Ts { get; set; }
            public double Value { get; set; }
        }

        // ===== POST Request DTOs para Series by-run =====

        /// <summary>
        /// Request body para endpoints de séries (voltage, current, frequency, dfreq, digital, thd).
        /// </summary>
        public sealed class SeriesByRunRequest
        {
            public Guid RunId { get; set; }
            public string? MaxPoints { get; set; } = "5000";
            public string? Unit { get; set; } = "raw";
            public bool? Tri { get; set; } = false;
            public string? Phase { get; set; }
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            public DateTime? From { get; set; }
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de sequências (seq/by-run).
        /// </summary>
        public sealed class SeqSeriesByRunRequest
        {
            public Guid RunId { get; set; }
            public string? MaxPoints { get; set; } = "5000";
            public string? Unit { get; set; } = "raw";
            public double? VoltLevel { get; set; }
            public string? Kind { get; set; } // voltage|current
            public string? Seq { get; set; } // pos|neg|zero
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            public DateTime? From { get; set; }
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de desequilíbrio (unbalance/by-run).
        /// </summary>
        public sealed class UnbalanceSeriesByRunRequest
        {
            public Guid RunId { get; set; }
            public string? MaxPoints { get; set; } = "5000";
            public double? VoltLevel { get; set; }
            public string? Kind { get; set; } // voltage|current
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            public DateTime? From { get; set; }
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de potência (power/by-run).
        /// </summary>
        public sealed class PowerSeriesByRunRequest
        {
            public Guid RunId { get; set; }
            public string? MaxPoints { get; set; } = "5000";
            public string? Which { get; set; } // active|reactive
            public string? Unit { get; set; } = "raw";
            public bool? Tri { get; set; } = false;
            public bool? Total { get; set; } = false;
            public string? Phase { get; set; }
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            public DateTime? From { get; set; }
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de diferença angular (angle-diff/by-run).
        /// </summary>
        public sealed class AngleDiffSeriesByRunRequest
        {
            public Guid RunId { get; set; }
            public string? MaxPoints { get; set; } = "5000";
            public string? Kind { get; set; } // voltage|current
            public string? Reference { get; set; } // PMU reference name (required)
            public string? Phase { get; set; } // A|B|C
            public string? Sequence { get; set; } // pos|neg|zero
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            public DateTime? From { get; set; }
            public DateTime? To { get; set; }
        }

    }
