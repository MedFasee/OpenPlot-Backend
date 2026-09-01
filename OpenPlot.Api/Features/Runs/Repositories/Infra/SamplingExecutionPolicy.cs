namespace OpenPlot.Features.Runs.Repositories;

public readonly record struct SamplingDecision(
    MeasurementsRepository.SamplingPlan Plan,
    MeasurementsRepository.SamplingSource Source,
    string Strategy);

/// <summary>
/// Ponto unico de decisao RAW_DB vs SAMPLED_DB, chamado por todas as familias
/// de series (Simple/Phasor/AngleFrames/PowerFrames). Por enquanto delega para
/// a mesma logica ja validada (BuildSamplingPlan/ResolveSamplingSource) sem
/// alterar nenhum criterio; estrategias futuras (ex.: RAW_DB_AND_SAMPLE_IN_MEMORY)
/// devem ser adicionadas aqui, nao dentro de um handler/familia especifica.
/// </summary>
public interface ISamplingExecutionPolicy
{
    SamplingDecision Decide(
        DateTime fromUtc,
        DateTime toUtc,
        int? maxPoints,
        TimeSpan minimumBucket,
        bool forceSampling,
        double expectedFps,
        bool usePreviewContinuousAggregates);
}

public sealed class SamplingExecutionPolicy : ISamplingExecutionPolicy
{
    public SamplingDecision Decide(
        DateTime fromUtc,
        DateTime toUtc,
        int? maxPoints,
        TimeSpan minimumBucket,
        bool forceSampling,
        double expectedFps,
        bool usePreviewContinuousAggregates)
    {
        var plan = MeasurementsRepository.BuildSamplingPlan(
            fromUtc,
            toUtc,
            maxPoints,
            minimumBucket,
            forceSampling,
            expectedFps);

        var source = MeasurementsRepository.ResolveSamplingSource(
            plan,
            usePreviewContinuousAggregates);

        return new SamplingDecision(
            plan,
            source,
            plan.UseRaw ? "RAW_DB" : "SAMPLED_DB");
    }
}
