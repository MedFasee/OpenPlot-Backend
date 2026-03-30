using Microsoft.AspNetCore.Mvc;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace OpenPlot.Data.Dtos
{
    /// <summary>
    /// Conversor customizado para aceitar maxPoints como número ou string.
    /// </summary>
    public class MaxPointsConverter : JsonConverter<string?>
    {
        public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.TokenType switch
            {
                JsonTokenType.String => reader.GetString(),
                JsonTokenType.Number => reader.GetInt64().ToString(),
                JsonTokenType.Null => null,
                _ => throw new JsonException($"Unexpected token {reader.TokenType} when parsing maxPoints")
            };
        }

        public override void Write(Utf8JsonWriter writer, string? value, JsonSerializerOptions options)
        {
            if (value is null)
                writer.WriteNullValue();
            else
                writer.WriteStringValue(value);
        }
    }

    /// <summary>
    /// Conversor customizado para aceitar Guid com múltiplos nomes de propriedade (runId, run_Id, run_id).
    /// </summary>
    public class FlexibleRunIdConverter : JsonConverter<Guid>
    {
        public override Guid Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.TokenType switch
            {
                JsonTokenType.String => Guid.Parse(reader.GetString() ?? ""),
                _ => throw new JsonException($"Unexpected token {reader.TokenType} when parsing RunId")
            };
        }

        public override void Write(Utf8JsonWriter writer, Guid value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }
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
        [FromQuery(Name = "run_id")]
        public Guid RunId { get; init; }

        public bool Tri { get; init; }
        public string? Phase { get; init; }
        public string? Unit { get; init; }
        public string? Kind { get; init; }

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
            [JsonPropertyName("run_id")]
            public Guid RunId { get; set; }
            [JsonConverter(typeof(MaxPointsConverter))]
            [JsonPropertyName("maxPoints")]
            public string? MaxPoints { get; set; } = "5000";
            [JsonPropertyName("unit")]
            public string? Unit { get; set; } = "raw";
            public string? Kind { get; set; } = "raw";
            [JsonPropertyName("tri")]
            public bool? Tri { get; set; } = false;
            [JsonPropertyName("phase")]
            public string? Phase { get; set; }
            [JsonPropertyName("pmu")]
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            [JsonPropertyName("from")]
            public DateTime? From { get; set; }
            [JsonPropertyName("to")]
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de sequências (seq/by-run).
        /// </summary>
        public sealed class SeqSeriesByRunRequest
        {
            [JsonPropertyName("run_id")]
            public Guid RunId { get; set; }
            [JsonConverter(typeof(MaxPointsConverter))]
            [JsonPropertyName("maxPoints")]
            public string? MaxPoints { get; set; } = "5000";
            [JsonPropertyName("unit")]
            public string? Unit { get; set; } = "raw";
            [JsonPropertyName("voltLevel")]
            public double? VoltLevel { get; set; }
            [JsonPropertyName("kind")]
            public string? Kind { get; set; } // voltage|current
            [JsonPropertyName("seq")]
            public string? Seq { get; set; } // pos|neg|zero
            [JsonPropertyName("pmu")]
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            [JsonPropertyName("from")]
            public DateTime? From { get; set; }
            [JsonPropertyName("to")]
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de desequilíbrio (unbalance/by-run).
        /// </summary>
        public sealed class UnbalanceSeriesByRunRequest
        {
            [JsonPropertyName("run_id")]
            public Guid RunId { get; set; }
            [JsonConverter(typeof(MaxPointsConverter))]
            [JsonPropertyName("maxPoints")]
            public string? MaxPoints { get; set; } = "5000";
            [JsonPropertyName("voltLevel")]
            public double? VoltLevel { get; set; }
            [JsonPropertyName("kind")]
            public string? Kind { get; set; } // voltage|current
            [JsonPropertyName("pmu")]
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            [JsonPropertyName("from")]
            public DateTime? From { get; set; }
            [JsonPropertyName("to")]
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de potência (power/by-run).
        /// </summary>
        public sealed class PowerSeriesByRunRequest
        {
            [JsonPropertyName("run_id")]
            public Guid RunId { get; set; }
            [JsonConverter(typeof(MaxPointsConverter))]
            [JsonPropertyName("maxPoints")]
            public string? MaxPoints { get; set; } = "5000";
            [JsonPropertyName("which")]
            public string? Which { get; set; } // active|reactive
            [JsonPropertyName("unit")]
            public string? Unit { get; set; } = "raw";
            [JsonPropertyName("tri")]
            public bool? Tri { get; set; } = false;
            [JsonPropertyName("total")]
            public bool? Total { get; set; } = false;
            [JsonPropertyName("phase")]
            public string? Phase { get; set; }
            [JsonPropertyName("pmu")]
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            [JsonPropertyName("from")]
            public DateTime? From { get; set; }
            [JsonPropertyName("to")]
            public DateTime? To { get; set; }
        }

        /// <summary>
        /// Request body para endpoint de diferença angular (angle-diff/by-run).
        /// </summary>
        public sealed class AngleDiffSeriesByRunRequest
        {
            [JsonPropertyName("run_id")]
            public Guid RunId { get; set; }
            [JsonConverter(typeof(MaxPointsConverter))]
            [JsonPropertyName("maxPoints")]
            public string? MaxPoints { get; set; } = "5000";
            [JsonPropertyName("kind")]
            public string? Kind { get; set; } // voltage|current
            [JsonPropertyName("ref")]
            public string? Reference { get; set; } // PMU reference name (required)
            [JsonPropertyName("phase")]
            public string? Phase { get; set; } // A|B|C
            [JsonPropertyName("seq")]
            public string? Sequence { get; set; } // pos|neg|zero
            [JsonPropertyName("sequence")]
            public string? SequenceAlias
            {
                get => Sequence;
                set => Sequence = value;
            }
            [JsonPropertyName("pmu")]
            public string[]? Pmu { get; set; } = Array.Empty<string>();
            [JsonPropertyName("from")]
            public DateTime? From { get; set; }
            [JsonPropertyName("to")]
            public DateTime? To { get; set; }
        }

    }
