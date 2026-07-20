using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;
using static ConfigEndpoints;

// DTO para teste de avarias
public class RunIngestaTest
{
    public Guid id { get; set; }
    public string? ids { get; set; }
}

public static class RunsEndpoints
{
    internal static SearchRunItem CreateSearchRunItem(SearchRunRow r, string label) => new()
    {
        label = label,
        status = r.status,
        id = r.id.ToString(),
        shared = r.shared,
        owner = r.owner,
        conv_comtrade = r.conv_comtrade
    };

    internal static ByRunQuery BuildByRunQuery(SeriesByRunRequest request) =>
        new()
        {
            RunId = request.RunId,
            Tri = request.Tri ?? false,
            Phase = request.Phase,
            Unit = request.Unit,
            Pmus = request.Pmu,
            MaxPoints = request.MaxPoints,
            PreviewOnly = request.PreviewOnly ?? false
        };

    internal static ByRunQuery BuildThdByRunQuery(SeriesByRunRequest request) =>
        new()
        {
            RunId = request.RunId,
            Tri = request.Tri ?? false,
            Phase = request.Phase,
            Unit = "%",
            Pmu = request.Tri == true ? request.Pmu?.FirstOrDefault() : null,
            Pmus = request.Tri == false ? request.Pmu : null,
            MaxPoints = request.MaxPoints,
            PreviewOnly = request.PreviewOnly ?? false
        };

    internal static SeqRunQuery BuildSeqRunQuery(SeqSeriesByRunRequest request) =>
        new(
            RunId: request.RunId,
            MaxPoints: request.MaxPoints,
            PreviewOnly: request.PreviewOnly ?? false,
            Unit: request.Unit,
            VoltLevel: request.VoltLevel,
            Seq: request.Seq,
            Kind: request.Kind);

    internal static UnbalanceRunQuery BuildUnbalanceRunQuery(UnbalanceSeriesByRunRequest request) =>
        new(
            RunId: request.RunId,
            MaxPoints: request.MaxPoints,
            PreviewOnly: request.PreviewOnly ?? false,
            VoltLevel: request.VoltLevel,
            Kind: request.Kind);

    internal static PowerPlotQuery BuildPowerPlotQuery(PowerSeriesByRunRequest request) =>
        new()
        {
            RunId = request.RunId,
            Pmu = request.Pmu,
            Which = request.Which,
            Unit = request.Unit,
            MaxPoints = request.MaxPoints,
            Tri = request.Tri,
            Total = request.Total,
            Phase = request.Phase,
            PreviewOnly = request.PreviewOnly ?? false
        };

    internal static AngleDiffQuery BuildAngleDiffQuery(AngleDiffSeriesByRunRequest request) =>
        new()
        {
            RunId = request.RunId,
            MaxPoints = request.MaxPoints,
            PreviewOnly = request.PreviewOnly ?? false,
            Kind = request.Kind,
            Reference = request.Reference,
            Phase = request.Phase,
            Sequence = request.Sequence
        };

    internal static Dictionary<string, object?>? BuildOscillationModes(IUiMenuService uiMenus) =>
        uiMenus.Build(UiMenuSet.Oscillations);

    internal static SeqRequest BuildSeqRequest(SeqRunQuery query) =>
        new(
            Kind: (query.Kind ?? string.Empty).Trim().ToLowerInvariant() == "current"
                ? SeqKind.Current
                : SeqKind.Voltage,
            Seq: (query.Seq ?? string.Empty).Trim().ToLowerInvariant() switch
            {
                "pos" or "seq+" or "1" => SeqType.Pos,
                "neg" or "seq-" or "2" => SeqType.Neg,
                "zero" or "seq0" or "0" => SeqType.Zero,
                _ => throw new BadHttpRequestException("seq inválida (pos|neg|zero).")
            });

    internal static UnbalanceRequest BuildUnbalanceRequest(UnbalanceRunQuery query) =>
        new((query.Kind ?? string.Empty).Trim().ToLowerInvariant() == "current"
            ? SeqKind.Current
            : SeqKind.Voltage);

    public static IEndpointRouteBuilder MapRuns(this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("")
                     .WithTags("Runs").RequireAuthorization();

        grp.MapRunsListing();
        grp.MapRunsTerminals();
        grp.MapRunsSimpleSeries();
        grp.MapRunsPhasorSeries();
        grp.MapRunsAnalyticalSeries();
        grp.MapRunsAdvancedSeries();

        return app;
    }
}
