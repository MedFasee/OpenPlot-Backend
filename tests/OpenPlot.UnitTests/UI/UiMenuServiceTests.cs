using OpenPlot.Services.UI;

namespace OpenPlot.UnitTests.UI;

public sealed class UiMenuServiceTests
{
    private static readonly FeatureFlags EnabledFlags = new(
        EnablesDFT: true,
        EnablesProny: true,
        EnablesCCA: true,
        EnablesEventsAnalyzer: true);

    [Fact]
    public void Build_WhenContextIsNull_ReturnsCcaDefaultsWithEnabled()
    {
        var sut = new UiMenuService(EnabledFlags);

        var result = sut.Build(UiMenuSet.Oscillations);

        var oscillations = Assert.IsType<Dictionary<string, object?>>(Assert.Contains("oscillations", result!));
        var environment = Assert.IsType<Dictionary<string, object?>>(Assert.Contains("Ambiente", oscillations));
        var cca = Assert.IsType<Dictionary<string, object?>>(Assert.Contains("CCA", environment));

        Assert.True(Assert.IsType<bool>(Assert.Contains("enabled", cca)));
        Assert.Equal(8, Assert.IsType<int>(Assert.Contains("Ordem do modelo", cca)));
        Assert.Equal(20, Assert.IsType<int>(Assert.Contains("N° de linhas por bloco", cca)));
        Assert.Equal(10, Assert.IsType<int>(Assert.Contains("Tam. da janela (min.)", cca)));
        Assert.Equal(60, Assert.IsType<int>(Assert.Contains("Passo da janela (s)", cca)));
        Assert.Equal(0.3, Assert.IsType<double>(Assert.Contains("Freq. mínima (Hz)", cca)), 10);
        Assert.Equal(0.4, Assert.IsType<double>(Assert.Contains("Freq. máxima (Hz)", cca)), 10);
    }

    [Fact]
    public void Build_WhenContextHasEnoughPoints_EnablesCca()
    {
        var sut = new UiMenuService(EnabledFlags);
        var context = new UiMenuContext(
            WindowFromUtc: new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            WindowToUtc: new DateTime(2025, 1, 1, 0, 20, 0, DateTimeKind.Utc),
            SelectRate: 1,
            EffectivePointCount: 1201,
            TotalSeriesCount: 2,
            ValidSeriesCount: 2,
            AvailablePointCount: 1201);

        var result = sut.Build(UiMenuSet.Oscillations, context);

        var cca = GetCcaSettings(result!);
        Assert.True(Assert.IsType<bool>(Assert.Contains("enabled", cca)));
    }

    [Fact]
    public void Build_WhenWindowDefaultsDoNotFitAvailablePoints_DisablesCca()
    {
        var sut = new UiMenuService(EnabledFlags);
        var context = new UiMenuContext(
            WindowFromUtc: new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            WindowToUtc: new DateTime(2025, 1, 1, 0, 5, 0, DateTimeKind.Utc),
            SelectRate: 1,
            EffectivePointCount: 301,
            TotalSeriesCount: 1,
            ValidSeriesCount: 1,
            AvailablePointCount: 301);

        var result = sut.Build(UiMenuSet.Oscillations, context);

        var cca = GetCcaSettings(result!);
        Assert.False(Assert.IsType<bool>(Assert.Contains("enabled", cca)));
    }

    [Fact]
    public void RebuildForRun_WhenCacheContextHasNoValidSeries_DisablesCca()
    {
        var sut = new UiMenuService(EnabledFlags);
        var initialModes = sut.Build(UiMenuSet.Oscillations)!;
        var context = new UiMenuContext(
            WindowFromUtc: new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            WindowToUtc: new DateTime(2025, 1, 1, 0, 20, 0, DateTimeKind.Utc),
            SelectRate: 10,
            EffectivePointCount: 12001,
            TotalSeriesCount: 2,
            ValidSeriesCount: 0,
            AvailablePointCount: 12001);

        var rebuilt = sut.RebuildForRun(initialModes, context);

        var cca = GetCcaSettings(rebuilt!);
        Assert.False(Assert.IsType<bool>(Assert.Contains("enabled", cca)));
    }

    private static Dictionary<string, object?> GetCcaSettings(Dictionary<string, object?> result)
    {
        var oscillations = Assert.IsType<Dictionary<string, object?>>(Assert.Contains("oscillations", result));
        var environment = Assert.IsType<Dictionary<string, object?>>(Assert.Contains("Ambiente", oscillations));
        return Assert.IsType<Dictionary<string, object?>>(Assert.Contains("CCA", environment));
    }
}
