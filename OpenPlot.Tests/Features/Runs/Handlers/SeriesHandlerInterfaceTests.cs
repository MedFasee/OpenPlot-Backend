using Xunit;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;

namespace OpenPlot.Tests.Features.Runs.Handlers;

/// <summary>
/// Testes para interfaces de abstrações de handlers de séries.
/// Valida contrato esperado das implementações.
/// </summary>
public class SeriesHandlerInterfaceTests
{
    /// <summary>
    /// Implementação fake de ISeriesQuery para testes.
    /// </summary>
    private class FakeSeriesQuery : ISeriesQuery
    {
        public Guid RunId { get; set; } = Guid.NewGuid();
        public string? MaxPoints { get; set; }

        public bool MaxPointsIsAll =>
            string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

        public int ResolveMaxPoints(int @default = 5000)
        {
            if (MaxPointsIsAll) return int.MaxValue;
            if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
            return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
        }
    }

    [Fact]
    public void ISeriesQuery_WithAllKeyword_ShouldReturnIntMaxValue()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = "all" };

        // Act
        var result = query.ResolveMaxPoints();

        // Assert
        Assert.Equal(int.MaxValue, result);
    }

    [Fact]
    public void ISeriesQuery_WithValidNumber_ShouldReturnNumber()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = "2500" };

        // Act
        var result = query.ResolveMaxPoints();

        // Assert
        Assert.Equal(2500, result);
    }

    [Fact]
    public void ISeriesQuery_WithNullMaxPoints_ShouldReturnDefault()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = null };
        int defaultValue = 7500;

        // Act
        var result = query.ResolveMaxPoints(@default: defaultValue);

        // Assert
        Assert.Equal(defaultValue, result);
    }

    [Fact]
    public void ISeriesQuery_WithInvalidNumber_ShouldReturnDefault()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = "invalid" };

        // Act
        var result = query.ResolveMaxPoints(@default: 5000);

        // Assert
        Assert.Equal(5000, result);
    }

    [Fact]
    public void ISeriesQuery_WithNegativeNumber_ShouldReturnDefault()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = "-100" };

        // Act
        var result = query.ResolveMaxPoints(@default: 5000);

        // Assert
        Assert.Equal(5000, result);
    }

    [Fact]
    public void ISeriesQuery_MaxPointsIsAll_WithAllKeyword_ShouldBeTrue()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = "all" };

        // Act & Assert
        Assert.True(query.MaxPointsIsAll);
    }

    [Fact]
    public void ISeriesQuery_MaxPointsIsAll_WithNumber_ShouldBeFalse()
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = "5000" };

        // Act & Assert
        Assert.False(query.MaxPointsIsAll);
    }

    [Theory]
    [InlineData("ALL")]
    [InlineData("All")]
    [InlineData("aLl")]
    public void ISeriesQuery_MaxPointsIsAll_CaseInsensitive(string value)
    {
        // Arrange
        ISeriesQuery query = new FakeSeriesQuery { MaxPoints = value };

        // Act & Assert
        Assert.True(query.MaxPointsIsAll);
    }

    [Fact]
    public void ISeriesQuery_RunId_ShouldBeStorable()
    {
        // Arrange
        var runId = Guid.NewGuid();
        ISeriesQuery query = new FakeSeriesQuery { RunId = runId };

        // Act & Assert
        Assert.Equal(runId, query.RunId);
    }
}
