namespace OpenPlot.Features.Runs.Handlers.Abstractions;

/// <summary>
/// Interface para estratégias de downsampling de séries temporais.
/// Permite diferentes algoritmos mantendo API consistente.
/// </summary>
public interface IDownsamplingStrategy
{
    /// <summary>
    /// Reduz quantidade de pontos preservando min/max por bucket temporal.
    /// </summary>
    /// <param name="points">Pontos originais ordenados por timestamp.</param>
    /// <param name="maxPoints">Máximo de pontos desejado após downsampling.</param>
    /// <returns>Lista de pontos downsampleados.</returns>
    List<(DateTime ts, double val)> Downsample(
        IEnumerable<(DateTime ts, double val)> points,
        int maxPoints);
}

/// <summary>
/// Interface para estratégias de alinhamento temporal de séries (sincronização).
/// Essencial para operações que requerem múltiplas séries alinhadas.
/// </summary>
public interface ITimeSeriesAligner
{
    /// <summary>
    /// Alinha múltiplas séries no mesmo timeline dentro de tolerância.
    /// </summary>
    /// <param name="series">Dicionário com chaves arbitrárias e listas de pontos.</param>
    /// <param name="tolerance">Tolerância temporal para considerar pontos alinhados.</param>
    /// <returns>
    /// Dicionário com timestamps comuns e values alinhados por chave.
    /// Se uma chave não tem ponto próximo, pode ser omitida ou replicada conforme estratégia.
    /// </returns>
    Dictionary<DateTime, Dictionary<string, double>> Align(
        Dictionary<string, List<(DateTime ts, double val)>> series,
        TimeSpan tolerance);
}

/// <summary>
/// Interface para cálculos de potência (ativa/reativa) a partir de V/I fasoriais.
/// Encapsula lógica complexa de transformações.
/// </summary>
public interface IPowerCalculator
{
    /// <summary>
    /// Calcula potência ativa ou reativa para uma fase.
    /// </summary>
    /// <param name="voltMag">Série de magnitude de tensão.</param>
    /// <param name="voltAng">Série de ângulo de tensão.</param>
    /// <param name="currMag">Série de magnitude de corrente.</param>
    /// <param name="currAng">Série de ângulo de corrente.</param>
    /// <param name="tolerance">Tolerância para alinhamento temporal.</param>
    /// <param name="powerType">"active" para potência ativa, "reactive" para reativa.</param>
    /// <returns>Lista de potências calculadas com timestamps alinhados.</returns>
    List<(DateTime ts, double val)> CalculatePower(
        List<(DateTime ts, double val)> voltMag,
        List<(DateTime ts, double val)> voltAng,
        List<(DateTime ts, double val)> currMag,
        List<(DateTime ts, double val)> currAng,
        TimeSpan tolerance,
        string powerType);
}

/// <summary>
/// Interface para cálculos de sequências (positiva, negativa, zero).
/// Usa transformação simétrica de componentes trifásicos.
/// </summary>
public interface ISequenceCalculator
{
    /// <summary>
    /// Calcula componente de sequência a partir de magnitudes e ângulos trifásicos.
    /// </summary>
    /// <param name="magA">Magnitude da fase A ao longo do tempo.</param>
    /// <param name="magB">Magnitude da fase B ao longo do tempo.</param>
    /// <param name="magC">Magnitude da fase C ao longo do tempo.</param>
    /// <param name="angA">Ângulo da fase A ao longo do tempo.</param>
    /// <param name="angB">Ângulo da fase B ao longo do tempo.</param>
    /// <param name="angC">Ângulo da fase C ao longo do tempo.</param>
    /// <param name="sequenceType">"pos" (positiva), "neg" (negativa), "zero".</param>
    /// <param name="tolerance">Tolerância para alinhamento temporal.</param>
    /// <returns>Lista com magnitude e ângulo da sequência.</returns>
    (
        List<(DateTime ts, double val)> magnitude,
        List<(DateTime ts, double val)> angle
    ) CalculateSequence(
        List<(DateTime ts, double val)> magA,
        List<(DateTime ts, double val)> magB,
        List<(DateTime ts, double val)> magC,
        List<(DateTime ts, double val)> angA,
        List<(DateTime ts, double val)> angB,
        List<(DateTime ts, double val)> angC,
        string sequenceType,
        TimeSpan tolerance);
}
