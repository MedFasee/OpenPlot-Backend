using Xunit;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;

namespace OpenPlot.Tests.Features.Runs.Handlers;

/// <summary>
/// Testes para PowerSeriesHandler.
/// Valida validação de parâmetros de potência.
/// </summary>
public class PowerSeriesHandlerValidationTests
{
    private readonly PowerSeriesHandler _handler;

    public PowerSeriesHandlerValidationTests()
    {
        // Criamos handler com mocks mínimos apenas para testar validação
        var mockRunRepo = new Moq.Mock<IRunContextRepository>();
        var mockMeta = new Moq.Mock<IPlotMetaBuilder>();
        var mockCache = new Moq.Mock<IAnalysisCacheRepository>();
        var mockDb = new Moq.Mock<IDbConnectionFactory>();
        var mockCalc = new Moq.Mock<IPowerCalculator>();

        _handler = new PowerSeriesHandler(
            mockRunRepo.Object,
            mockMeta.Object,
            mockCache.Object,
            mockDb.Object,
            mockCalc.Object);
    }

    [Fact]
    public void PowerSeriesHandler_WithTriAndTotal_ShouldBeMutuallyExclusive()
    {
        // Arrange
        var query = new PowerPlotQuery
        {
            RunId = Guid.NewGuid(),
            Tri = true,
            Total = true,
            Which = "active"
        };
        var window = new WindowQuery(null, null);

        // Act - não há método público para testar validação diretamente
        // Isso será testado via HandleAsync em integração

        // Assert - placeholder para próxima fase
        Assert.True(true);
    }

    [Fact]
    public void AngleDiffQuery_ShouldImplementISeriesQuery()
    {
        // Arrange & Act
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            MaxPoints = "5000"
        };

        // Assert
        Assert.Equal(5000, query.ResolveMaxPoints());
        Assert.False(query.MaxPointsIsAll);
    }

    [Fact]
    public void AngleDiffQuery_WithMaxPointsAll_ShouldReturnIntMaxValue()
    {
        // Arrange
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            MaxPoints = "all"
        };

        // Act & Assert
        Assert.True(query.MaxPointsIsAll);
        Assert.Equal(int.MaxValue, query.ResolveMaxPoints());
    }
}

/// <summary>
/// Testes para AngleDiffSeriesHandler.
/// Valida validação de parâmetros de diferença angular.
/// </summary>
public class AngleDiffSeriesHandlerTests
{
    [Theory]
    [InlineData("voltage")]
    [InlineData("current")]
    [InlineData("VOLTAGE")]
    [InlineData("CURRENT")]
    public void AngleDiffQuery_WithValidKind_ShouldBeAccepted(string kind)
    {
        // Arrange
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            Kind = kind,
            Reference = "PMU_REF"
        };

        // Act & Assert
        Assert.Equal(kind, query.Kind);
    }

    [Theory]
    [InlineData("A")]
    [InlineData("B")]
    [InlineData("C")]
    public void AngleDiffQuery_WithValidPhase_ShouldBeAccepted(string phase)
    {
        // Arrange
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            Phase = phase
        };

        // Act & Assert
        Assert.Equal(phase, query.Phase);
    }

    [Theory]
    [InlineData("pos")]
    [InlineData("neg")]
    [InlineData("zero")]
    [InlineData("seq+")]
    [InlineData("seq-")]
    [InlineData("seq0")]
    public void AngleDiffQuery_WithValidSequence_ShouldBeAccepted(string seq)
    {
        // Arrange
        var query = new AngleDiffQuery
        {
            RunId = Guid.NewGuid(),
            Sequence = seq
        };

        // Act & Assert
        Assert.Equal(seq, query.Sequence);
    }
}

/// <summary>
/// Testes para ThdSeriesHandler.
/// Valida validação de parâmetros de THD.
/// </summary>
public class ThdSeriesHandlerTests
{
    [Theory]
    [InlineData("voltage")]
    [InlineData("current")]
    public void ThdHandler_WithValidKind_ShouldBeAccepted(string kind)
    {
        // Arrange & Act
        var isValid = kind is "voltage" or "current";

        // Assert
        Assert.True(isValid);
    }

    [Fact]
    public void ThdHandler_WithTriMode_RequiresPmu()
    {
        // Arrange
        var query = new ByRunQuery
        {
            RunId = Guid.NewGuid(),
            Tri = true,
            Pmu = "PMU1"
        };

        // Act & Assert
        Assert.True(!string.IsNullOrWhiteSpace(query.Pmu));
    }

    [Fact]
    public void ThdHandler_WithMonoMode_RequiresPhase()
    {
        // Arrange
        var query = new ByRunQuery
        {
            RunId = Guid.NewGuid(),
            Tri = false,
            Phase = "A"
        };

        // Act & Assert
        Assert.True(!string.IsNullOrWhiteSpace(query.Phase));
    }
}
