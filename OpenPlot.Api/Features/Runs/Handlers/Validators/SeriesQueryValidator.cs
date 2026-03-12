namespace OpenPlot.Features.Runs.Handlers.Validators;

/// <summary>
/// Resultado de validação.
/// </summary>
public sealed record ValidationResult(bool IsValid, string? ErrorMessage)
{
    public static ValidationResult Success() => new(true, null);
    public static ValidationResult Failure(string message) => new(false, message);
}

/// <summary>
/// Validador centralizado para queries de séries.
/// Encapsula regras de negócio comuns.
/// </summary>
public static class SeriesQueryValidator
{
    /// <summary>
    /// Valida RunId (deve ser não-vazio).
    /// </summary>
    public static ValidationResult ValidateRunId(Guid runId)
    {
        return runId == Guid.Empty
            ? ValidationResult.Failure("run_id é obrigatório.")
            : ValidationResult.Success();
    }

    /// <summary>
    /// Valida janela temporal (from < to).
    /// </summary>
    public static ValidationResult ValidateTimeWindow(DateTime? from, DateTime? to)
    {
        if (from.HasValue && to.HasValue && from >= to)
            return ValidationResult.Failure("from deve ser menor que to.");

        return ValidationResult.Success();
    }

    /// <summary>
    /// Valida parâmetro de fase (A|B|C).
    /// </summary>
    public static ValidationResult ValidatePhase(string? phase, bool isRequired = true)
    {
        if (string.IsNullOrWhiteSpace(phase))
            return isRequired
                ? ValidationResult.Failure("phase é obrigatório (A|B|C).")
                : ValidationResult.Success();

        var normalized = phase.Trim().ToUpperInvariant();
        return normalized is "A" or "B" or "C"
            ? ValidationResult.Success()
            : ValidationResult.Failure("phase deve ser A, B ou C.");
    }

    /// <summary>
    /// Valida modo tri (trifásico).
    /// </summary>
    public static ValidationResult ValidateTriMode(bool tri, string? pmu)
    {
        if (!tri)
            return ValidationResult.Success();

        return string.IsNullOrWhiteSpace(pmu)
            ? ValidationResult.Failure("pmu é obrigatório quando tri=true.")
            : ValidationResult.Success();
    }

    /// <summary>
    /// Valida modo mono (monofásico).
    /// </summary>
    public static ValidationResult ValidateMonoMode(bool tri, string? phase, string[]? pmuArray)
    {
        if (tri)
            return ValidationResult.Success();

        // Phase é obrigatório em mono
        var phaseValidation = ValidatePhase(phase, isRequired: true);
        if (!phaseValidation.IsValid)
            return phaseValidation;

        // PMU é obrigatório em mono
        if (pmuArray is not { Length: > 0 })
            return ValidationResult.Failure("ao menos uma PMU é obrigatória no modo mono.");

        return ValidationResult.Success();
    }

    /// <summary>
    /// Valida unidade de tensão (raw|pu).
    /// </summary>
    public static ValidationResult ValidateVoltageUnit(string? unit)
    {
        if (string.IsNullOrWhiteSpace(unit))
            return ValidationResult.Success(); // Default: raw

        var normalized = unit.Trim().ToLowerInvariant();
        return normalized is "raw" or "pu"
            ? ValidationResult.Success()
            : ValidationResult.Failure("unit deve ser 'raw' ou 'pu'.");
    }

    /// <summary>
    /// Valida tipo de potência (active|reactive).
    /// </summary>
    public static ValidationResult ValidatePowerType(string? which)
    {
        if (string.IsNullOrWhiteSpace(which))
            return ValidationResult.Success(); // Default: active

        var normalized = which.Trim().ToLowerInvariant();
        return normalized is "active" or "reactive"
            ? ValidationResult.Success()
            : ValidationResult.Failure("which deve ser 'active' ou 'reactive'.");
    }

    /// <summary>
    /// Valida unidade de potência (raw|mw).
    /// </summary>
    public static ValidationResult ValidatePowerUnit(string? unit)
    {
        if (string.IsNullOrWhiteSpace(unit))
            return ValidationResult.Success(); // Default: raw

        var normalized = unit.Trim().ToLowerInvariant();
        return normalized is "raw" or "mw"
            ? ValidationResult.Success()
            : ValidationResult.Failure("unit deve ser 'raw' ou 'mw'.");
    }

    /// <summary>
    /// Valida tipo de kind (voltage|current).
    /// </summary>
    public static ValidationResult ValidateKind(string? kind, bool isRequired = true)
    {
        if (string.IsNullOrWhiteSpace(kind))
            return isRequired
                ? ValidationResult.Failure("kind é obrigatório (voltage|current).")
                : ValidationResult.Success();

        var normalized = kind.Trim().ToLowerInvariant();
        return normalized is "voltage" or "current"
            ? ValidationResult.Success()
            : ValidationResult.Failure("kind deve ser 'voltage' ou 'current'.");
    }

    /// <summary>
    /// Valida parâmetro de sequência (pos|neg|zero).
    /// </summary>
    public static ValidationResult ValidateSequence(string? seq, bool isRequired = true)
    {
        if (string.IsNullOrWhiteSpace(seq))
            return isRequired
                ? ValidationResult.Failure("seq é obrigatório (pos|neg|zero).")
                : ValidationResult.Success();

        var normalized = seq.Trim().ToLowerInvariant();
        var valid = normalized switch
        {
            "pos" or "seq+" or "1" => true,
            "neg" or "seq-" or "2" => true,
            "zero" or "seq0" or "0" => true,
            _ => false
        };

        return valid
            ? ValidationResult.Success()
            : ValidationResult.Failure("seq inválida. Use pos|neg|zero (ou seq+|seq-|seq0).");
    }

    /// <summary>
    /// Valida exclusividade entre dois parâmetros (XOR).
    /// </summary>
    public static ValidationResult ValidateExclusivity(string? param1, string? param2, string param1Name, string param2Name)
    {
        var has1 = !string.IsNullOrWhiteSpace(param1);
        var has2 = !string.IsNullOrWhiteSpace(param2);

        if (has1 == has2)
            return ValidationResult.Failure($"informe exatamente um: {param1Name} ou {param2Name}.");

        return ValidationResult.Success();
    }

    /// <summary>
    /// Valida exclusividade entre dois booleans (não podem ser ambos true).
    /// </summary>
    public static ValidationResult ValidateBoolExclusivity(bool val1, bool val2, string name1, string name2)
    {
        if (val1 && val2)
            return ValidationResult.Failure($"{name1}=true e {name2}=true são mutuamente exclusivos.");

        return ValidationResult.Success();
    }
}
