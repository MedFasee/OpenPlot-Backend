using Xunit;
using OpenPlot.Features.Runs.Handlers.Validators;

namespace OpenPlot.Tests.Features.Runs.Handlers;

/// <summary>
/// Testes para validador centralizado SeriesQueryValidator.
/// Testa cada regra de validação isoladamente.
/// </summary>
public class SeriesQueryValidatorTests
{
    [Fact]
    public void ValidateRunId_WithEmptyGuid_ShouldFail()
    {
        // Act
        var result = SeriesQueryValidator.ValidateRunId(Guid.Empty);

        // Assert
        Assert.False(result.IsValid);
        Assert.NotNull(result.ErrorMessage);
    }

    [Fact]
    public void ValidateRunId_WithValidGuid_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateRunId(Guid.NewGuid());

        // Assert
        Assert.True(result.IsValid);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void ValidateTimeWindow_WithFromGreaterThanTo_ShouldFail()
    {
        // Arrange
        var now = DateTime.UtcNow;

        // Act
        var result = SeriesQueryValidator.ValidateTimeWindow(now, now.AddMinutes(-1));

        // Assert
        Assert.False(result.IsValid);
    }

    [Fact]
    public void ValidateTimeWindow_WithFromLessThanTo_ShouldPass()
    {
        // Arrange
        var now = DateTime.UtcNow;

        // Act
        var result = SeriesQueryValidator.ValidateTimeWindow(now, now.AddMinutes(1));

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateTimeWindow_WithNullValues_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateTimeWindow(null, null);

        // Assert
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("A")]
    [InlineData("B")]
    [InlineData("C")]
    [InlineData("a")]
    [InlineData("b")]
    [InlineData("c")]
    public void ValidatePhase_WithValidPhase_ShouldPass(string phase)
    {
        // Act
        var result = SeriesQueryValidator.ValidatePhase(phase);

        // Assert
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("X")]
    [InlineData("D")]
    [InlineData("")]
    [InlineData("abc")]
    public void ValidatePhase_WithInvalidPhase_ShouldFail(string phase)
    {
        // Act
        var result = SeriesQueryValidator.ValidatePhase(phase, isRequired: true);

        // Assert
        Assert.False(result.IsValid);
    }

    [Fact]
    public void ValidatePhase_WithNullAndNotRequired_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidatePhase(null, isRequired: false);

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateTriMode_WithTriFalse_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateTriMode(false, null);

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateTriMode_WithTriTrueAndValidPmu_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateTriMode(true, "PMU1");

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateTriMode_WithTriTrueAndNoPmu_ShouldFail()
    {
        // Act
        var result = SeriesQueryValidator.ValidateTriMode(true, null);

        // Assert
        Assert.False(result.IsValid);
    }

    [Theory]
    [InlineData("raw")]
    [InlineData("pu")]
    [InlineData("RAW")]
    [InlineData("PU")]
    public void ValidateVoltageUnit_WithValidUnit_ShouldPass(string unit)
    {
        // Act
        var result = SeriesQueryValidator.ValidateVoltageUnit(unit);

        // Assert
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("volt")]
    [InlineData("kv")]
    [InlineData("invalid")]
    public void ValidateVoltageUnit_WithInvalidUnit_ShouldFail(string unit)
    {
        // Act
        var result = SeriesQueryValidator.ValidateVoltageUnit(unit);

        // Assert
        Assert.False(result.IsValid);
    }

    [Theory]
    [InlineData("active")]
    [InlineData("reactive")]
    [InlineData("ACTIVE")]
    [InlineData("REACTIVE")]
    public void ValidatePowerType_WithValidType_ShouldPass(string which)
    {
        // Act
        var result = SeriesQueryValidator.ValidatePowerType(which);

        // Assert
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("passive")]
    [InlineData("imaginary")]
    public void ValidatePowerType_WithInvalidType_ShouldFail(string which)
    {
        // Act
        var result = SeriesQueryValidator.ValidatePowerType(which);

        // Assert
        Assert.False(result.IsValid);
    }

    [Theory]
    [InlineData("raw")]
    [InlineData("mw")]
    [InlineData("RAW")]
    [InlineData("MW")]
    public void ValidatePowerUnit_WithValidUnit_ShouldPass(string unit)
    {
        // Act
        var result = SeriesQueryValidator.ValidatePowerUnit(unit);

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateKind_WithValidKind_ShouldPass()
    {
        // Act
        var result1 = SeriesQueryValidator.ValidateKind("voltage");
        var result2 = SeriesQueryValidator.ValidateKind("current");

        // Assert
        Assert.True(result1.IsValid);
        Assert.True(result2.IsValid);
    }

    [Fact]
    public void ValidateKind_WithInvalidKind_ShouldFail()
    {
        // Act
        var result = SeriesQueryValidator.ValidateKind("power");

        // Assert
        Assert.False(result.IsValid);
    }

    [Theory]
    [InlineData("pos")]
    [InlineData("neg")]
    [InlineData("zero")]
    [InlineData("seq+")]
    [InlineData("seq-")]
    [InlineData("seq0")]
    [InlineData("1")]
    [InlineData("2")]
    [InlineData("0")]
    public void ValidateSequence_WithValidSequence_ShouldPass(string seq)
    {
        // Act
        var result = SeriesQueryValidator.ValidateSequence(seq);

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateExclusivity_WithBothParameters_ShouldFail()
    {
        // Act
        var result = SeriesQueryValidator.ValidateExclusivity("value1", "value2", "param1", "param2");

        // Assert
        Assert.False(result.IsValid);
    }

    [Fact]
    public void ValidateExclusivity_WithOnlyParam1_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateExclusivity("value1", null, "param1", "param2");

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateExclusivity_WithOnlyParam2_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateExclusivity(null, "value2", "param1", "param2");

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateBoolExclusivity_WithBothTrue_ShouldFail()
    {
        // Act
        var result = SeriesQueryValidator.ValidateBoolExclusivity(true, true, "tri", "total");

        // Assert
        Assert.False(result.IsValid);
    }

    [Fact]
    public void ValidateBoolExclusivity_WithFirstTrue_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateBoolExclusivity(true, false, "tri", "total");

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidateBoolExclusivity_WithBothFalse_ShouldPass()
    {
        // Act
        var result = SeriesQueryValidator.ValidateBoolExclusivity(false, false, "tri", "total");

        // Assert
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ValidationResult_Success_ShouldHaveIsValidTrue()
    {
        // Act
        var result = ValidationResult.Success();

        // Assert
        Assert.True(result.IsValid);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void ValidationResult_Failure_ShouldHaveIsValidFalse()
    {
        // Act
        var result = ValidationResult.Failure("Error message");

        // Assert
        Assert.False(result.IsValid);
        Assert.Equal("Error message", result.ErrorMessage);
    }
}
