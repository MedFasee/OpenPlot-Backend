using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using OpenPlot.Services.BackgroundCache;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class AngleDiffQuery : ISeriesQuery
{
    public Guid RunId { get; init; }
    public string? MaxPoints { get; init; }
    public string? Kind { get; init; }       // voltage|current
    public string? Reference { get; init; }  // PMU referência
    public string? Phase { get; init; }      // A|B|C
    public string? Sequence { get; init; }   // pos|neg|zero

    public bool MaxPointsIsAll =>
        string.Equals(
            MaxPoints?.Trim(),
            "all",
            StringComparison.OrdinalIgnoreCase);

    public int ResolveMaxPoints(int @default = 5000)
    {
        if (MaxPointsIsAll)
            return int.MaxValue;

        if (string.IsNullOrWhiteSpace(MaxPoints))
            return @default;

        return int.TryParse(MaxPoints, out var n) && n > 0
            ? n
            : @default;
    }
}

/// <summary>
/// Diferença angular entre uma PMU de referência e as PMUs medidas.
///
/// A metodologia elétrica é mantida:
/// - diferença angular normalizada em [-180, 180];
/// - componentes simétricas com a=e^(j120°);
/// - tolerância temporal de 3 ms;
/// - fallback por retenção do último resultado válido.
///
/// Otimização:
/// - phase A/B/C usa somente a coluna angular da fase pedida;
/// - sequence usa um AngleFrameRow Wide por PMU/timestamp;
/// - maxPoints é aplicado no repository antes do processamento;
/// - não há expansão em seis PhasorAbcRow;
/// - preview não sofre um segundo downsampling em memória;
/// - cache integral é preenchido em background.
/// </summary>
public sealed class AngleDiffSeriesHandler
{
    private readonly IMeasurementsRepository _measurementsRepository;
    private readonly IRunContextRepository _runRepository;
    private readonly ILogger<AngleDiffSeriesHandler> _logger;
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IPmuQueryHelper _pmuHelper;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly IPlotMetaBuilder _metaBuilder;
    private readonly IUiMenuService _uiMenus;
    private readonly IBackgroundCacheQueue _backgroundCacheQueue;
    private readonly IHttpContextAccessor _httpContextAccessor;

    public AngleDiffSeriesHandler(
        IRunContextRepository runRepository,
        IMeasurementsRepository measurementsRepository,
        IAnalysisCacheRepository cacheRepo,
        ITimeSeriesDownsampler downsampler,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly,
        IPlotMetaBuilder metaBuilder,
        IUiMenuService uiMenus,
        IBackgroundCacheQueue backgroundCacheQueue,
        IHttpContextAccessor httpContextAccessor,
        ILogger<AngleDiffSeriesHandler> logger)
    {
        _runRepository = runRepository
            ?? throw new ArgumentNullException(nameof(runRepository));

        _measurementsRepository = measurementsRepository
            ?? throw new ArgumentNullException(nameof(measurementsRepository));

        _cacheRepo = cacheRepo
            ?? throw new ArgumentNullException(nameof(cacheRepo));

        // Mantido na assinatura para não quebrar DI/testes antigos.
        _ = downsampler
            ?? throw new ArgumentNullException(nameof(downsampler));

        _pmuHelper = pmuHelper
            ?? throw new ArgumentNullException(nameof(pmuHelper));

        _seriesAssembly = seriesAssembly
            ?? throw new ArgumentNullException(nameof(seriesAssembly));

        _metaBuilder = metaBuilder
            ?? throw new ArgumentNullException(nameof(metaBuilder));

        _uiMenus = uiMenus
            ?? throw new ArgumentNullException(nameof(uiMenus));

        _backgroundCacheQueue = backgroundCacheQueue
            ?? throw new ArgumentNullException(nameof(backgroundCacheQueue));

        _httpContextAccessor = httpContextAccessor
            ?? throw new ArgumentNullException(nameof(httpContextAccessor));

        _logger = logger
            ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IResult> HandleAsync(
        AngleDiffQuery query,
        WindowQuery window,
        string[]? pmuArray,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var validation = ValidateInput(query);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        try
        {
            return await HandleCoreAsync(
                query,
                window,
                pmuArray,
                modes,
                ct);
        }
        catch (OperationCanceledException)
        {
            return Results.StatusCode(
                StatusCodes.Status408RequestTimeout);
        }
        catch (Exception)
        {
            return Results.StatusCode(
                StatusCodes.Status500InternalServerError);
        }
    }

    private async Task<IResult> HandleCoreAsync(
        AngleDiffQuery query,
        WindowQuery window,
        string[]? pmuArray,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var totalWatch = Stopwatch.StartNew();

        var kind = query.Kind!.Trim().ToLowerInvariant();
        var refPmu = query.Reference!.Trim();
        var hasPhase = !string.IsNullOrWhiteSpace(query.Phase);

        var normalizedPhase = hasPhase
            ? query.Phase!.Trim().ToUpperInvariant()
            : null;

        var normalizedSequence = hasPhase
            ? null
            : NormalizeSeq(query.Sequence!);

        var selectedTargets = _pmuHelper
            .NormalizeExcluding(refPmu, pmuArray)
            .ToList();

        IReadOnlyList<string>? queryPmus =
            selectedTargets.Count > 0
                ? _pmuHelper
                    .Normalize(selectedTargets, new[] { refPmu })
                    .ToList()
                : null;

        var maxPts = query.ResolveMaxPoints(@default: 5000);

        int? frontMaxPoints = query.MaxPointsIsAll
            ? null
            : maxPts;

        var fromUtc = window.FromUtc;
        var toUtc = window.ToUtc;

        if (fromUtc.HasValue &&
            toUtc.HasValue &&
            fromUtc.Value >= toUtc.Value)
        {
            return Results.BadRequest("from < to");
        }

        _logger.LogInformation(
            "[PROCESS][AngleDiff][START] runId={RunId} kind={Kind} reference={Reference} phase={Phase} sequence={Sequence} maxPoints={MaxPoints}",
            query.RunId,
            kind,
            refPmu,
            normalizedPhase,
            normalizedSequence,
            query.MaxPointsIsAll ? "all" : maxPts);

        var ctx = await _runRepository.ResolveAsync(
            query.RunId,
            fromUtc,
            toUtc,
            ct);

        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var queryWatch = Stopwatch.StartNew();

        var frames = await _measurementsRepository.QueryAngleFramesAsync(
            ctx,
            kind,
            queryPmus,
            fromUtc,
            toUtc,
            ct,
            frontMaxPoints,
            normalizedPhase);

        queryWatch.Stop();

        if (frames.Count == 0)
            return Results.NotFound(
                "Nenhuma série encontrada para este run/filtros.");

        var processWatch = Stopwatch.StartNew();

        var projection = BuildProjection(
            frames,
            kind,
            refPmu,
            selectedTargets,
            normalizedPhase,
            normalizedSequence,
            buildFrontSeries: true);

        processWatch.Stop();

        if (projection.Series.Count == 0)
        {
            return Results.BadRequest(
                "Nenhuma PMU pôde ser processada (faltam sinais ou alinhamento falhou).");
        }

        var modeLabel = hasPhase
            ? "phase"
            : "sequence";

        var componentLabel = hasPhase
            ? "angle_diff_phase"
            : "angle_diff_sequence";

        var cacheSeries = projection.CachePoints
            .GroupBy(
                x => x.pmuId,
                StringComparer.OrdinalIgnoreCase)
            .Select(g => _seriesAssembly.BuildCacheSeries(
                signalId: 0,
                pdcPmuId: 0,
                idName: g.Key,
                pdcName: ctx.PdcName,
                referenceTerminal: refPmu,
                unit: "deg",
                phase: hasPhase
                    ? normalizedPhase
                    : normalizedSequence,
                quantity: kind,
                component: componentLabel,
                points: g.Select(x => (x.ts, x.value))))
            .ToList();

        var cachePayload = _seriesAssembly.BuildCachePayload(
            projection.WindowFrom,
            projection.WindowTo,
            ctx.SelectRate ?? 0,
            cacheSeries,
            normalizeMissingFrames: false);

        var cacheId = Guid.NewGuid();

        // Preview não é mais persistido sincronicamente.
        // O cache integral entra na fila somente após o HTTP terminar.
        var bgRunId = query.RunId;
        var bgKind = kind;
        var bgReference = refPmu;
        var bgQueryPmus = queryPmus?.ToArray();
        var bgSelectedTargets = selectedTargets.ToArray();
        var bgPhase = normalizedPhase;
        var bgSequence = normalizedSequence;
        var bgFromUtc = fromUtc;
        var bgToUtc = toUtc;
        var bgHasPhase = hasPhase;
        var bgComponentLabel = componentLabel;

        BackgroundCacheWorkItem workItem;

        if (query.MaxPointsIsAll)
        {
            // O request já carregou massa integral. Evita reconsulta.
            var fullPayloadAlreadyLoaded = _seriesAssembly.BuildCachePayload(
                projection.WindowFrom,
                projection.WindowTo,
                ctx.SelectRate ?? 0,
                cacheSeries);

            workItem = new BackgroundCacheWorkItem(
                Name: "AngleDiff",
                RunId: bgRunId,
                CacheId: cacheId,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var cacheRepo =
                        sp.GetRequiredService<IAnalysisCacheRepository>();

                    await cacheRepo.SaveAsync(
                        cacheId,
                        bgRunId,
                        fullPayloadAlreadyLoaded,
                        bgCt);
                });
        }
        else
        {
            workItem = new BackgroundCacheWorkItem(
                Name: "AngleDiff",
                RunId: bgRunId,
                CacheId: cacheId,
                ExecuteAsync: async (sp, bgCt) =>
                {
                    var runRepository =
                        sp.GetRequiredService<IRunContextRepository>();

                    var measurementsRepository =
                        sp.GetRequiredService<IMeasurementsRepository>();

                    var seriesAssembly =
                        sp.GetRequiredService<ISeriesAssemblyService>();

                    var cacheRepo =
                        sp.GetRequiredService<IAnalysisCacheRepository>();

                    var logger =
                        sp.GetRequiredService<ILogger<AngleDiffSeriesHandler>>();

                    var bgCtx = await runRepository.ResolveAsync(
                        bgRunId,
                        bgFromUtc,
                        bgToUtc,
                        bgCt);

                    if (bgCtx is null)
                        throw new InvalidOperationException(
                            $"Run não encontrado durante cache integral: {bgRunId}");

                    // maxPoints=null => RAW integral.
                    var fullFrames =
                        await measurementsRepository.QueryAngleFramesAsync(
                            bgCtx,
                            bgKind,
                            bgQueryPmus,
                            bgFromUtc,
                            bgToUtc,
                            bgCt,
                            maxPoints: null,
                            phase: bgPhase);

                    if (fullFrames.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral AngleDiff sem frames. runId={bgRunId}");

                    // Mesma metodologia de cálculo do preview.
                    var fullProjection = BuildProjection(
                        fullFrames,
                        bgKind,
                        bgReference,
                        bgSelectedTargets,
                        bgPhase,
                        bgSequence,
                        buildFrontSeries: false);

                    if (fullProjection.CachePoints.Count == 0)
                        throw new InvalidOperationException(
                            $"Cache integral AngleDiff sem pontos processados. runId={bgRunId}");

                    var fullCacheSeries =
                        fullProjection.CachePoints
                            .GroupBy(
                                x => x.pmuId,
                                StringComparer.OrdinalIgnoreCase)
                            .Select(g => seriesAssembly.BuildCacheSeries(
                                signalId: 0,
                                pdcPmuId: 0,
                                idName: g.Key,
                                pdcName: bgCtx.PdcName,
                                referenceTerminal: bgReference,
                                unit: "deg",
                                phase: bgHasPhase
                                    ? bgPhase
                                    : bgSequence,
                                quantity: bgKind,
                                component: bgComponentLabel,
                                points: g.Select(x => (x.ts, x.value))))
                            .ToList();

                    // Massa integral; sem downsampling.
                    // normalizeMissingFrames default=true apenas mantém
                    // a política existente de hold-last para frames faltantes.
                    var fullPayload =
                        seriesAssembly.BuildCachePayload(
                            fullProjection.WindowFrom,
                            fullProjection.WindowTo,
                            bgCtx.SelectRate ?? 0,
                            fullCacheSeries);

                    await cacheRepo.SaveAsync(
                        cacheId,
                        bgRunId,
                        fullPayload,
                        bgCt);

                    logger.LogInformation(
                        "[BYRUN][AngleDiff][CACHE-FULL][PERSISTED] runId={RunId} cacheId={CacheId} frames={Frames}",
                        bgRunId,
                        cacheId,
                        fullFrames.Count);
                });
        }

        if (!_backgroundCacheQueue.ScheduleAfterResponse(
                _httpContextAccessor.HttpContext,
                workItem))
        {
            return Results.StatusCode(
                StatusCodes.Status503ServiceUnavailable);
        }

        var dataStr = projection.WindowFrom.Date.ToString(
            "dd/MM/yyyy",
            CultureInfo.InvariantCulture);

        var measQuery = new MeasurementsQuery(
            Quantity: kind,
            Component: componentLabel,
            PhaseMode: hasPhase
                ? PhaseMode.Single
                : PhaseMode.Any,
            Phase: normalizedPhase,
            PmuNames: selectedTargets.Count > 0
                ? selectedTargets
                : null,
            Unit: "deg",
            ReferenceTerminal: refPmu);

        var plotMeta = _metaBuilder.Build(
            new WindowQuery(fromUtc, toUtc),
            ctx,
            measQuery);

        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            UiMenuContext.FromCache(cachePayload));

        totalWatch.Stop();

        _logger.LogInformation(
            "[PROCESS][AngleDiff][END] runId={RunId} elapsedMs={ElapsedMs} queryMs={QueryMs} processMs={ProcessMs} cacheSchedule=after_http frames={Frames} pmuCount={PmuCount}",
            query.RunId,
            totalWatch.ElapsedMilliseconds,
            queryWatch.ElapsedMilliseconds,
            processWatch.ElapsedMilliseconds,
            frames.Count,
            projection.Series.Count);

        return Results.Ok(new
        {
            run_id = query.RunId,
            data = dataStr,
            kind,
            reference = refPmu,
            mode = modeLabel,
            phase = normalizedPhase,
            seq = normalizedSequence,
            unit = "deg",
            cache_id = cacheId.ToString(),
            pmu_count = projection.Series.Count,
            window = new
            {
                from = projection.WindowFrom,
                to = projection.WindowTo
            },
            modes = resolvedModes,
            plot_meta = new
            {
                title = plotMeta.Title,
                x_label = plotMeta.XLabel,
                y_label = plotMeta.YLabel
            },
            series = projection.Series
        });
    }

    private static AngleProjection BuildProjection(
        IReadOnlyList<AngleFrameRow> frames,
        string kind,
        string referencePmu,
        IReadOnlyList<string> selectedTargets,
        string? phase,
        string? sequence,
        bool buildFrontSeries)
    {
        var hasPhase = phase is not null;

        var refFrames = frames
            .Where(x => x.IdName.Equals(
                referencePmu,
                StringComparison.OrdinalIgnoreCase))
            .OrderBy(x => x.Ts)
            .ToList();

        if (refFrames.Count == 0)
            return AngleProjection.Empty;

        var refAngles = hasPhase
            ? ExtractPhaseSeries(refFrames, phase!)
            : CalculateSequenceSeries(refFrames, sequence!);

        if (refAngles.Count == 0)
            return AngleProjection.Empty;

        HashSet<string>? selectedTargetSet = null;

        if (selectedTargets.Count > 0)
        {
            selectedTargetSet = new HashSet<string>(
                selectedTargets,
                StringComparer.OrdinalIgnoreCase);
        }

        var groups = frames
            .Where(x =>
                !x.IdName.Equals(
                    referencePmu,
                    StringComparison.OrdinalIgnoreCase)
                &&
                (selectedTargetSet is null ||
                 selectedTargetSet.Contains(x.IdName)))
            .GroupBy(
                x => x.IdName,
                StringComparer.OrdinalIgnoreCase);

        var series = new List<object>();

        var cachePoints =
            new List<(string pmuId, DateTime ts, double value)>();

        var tolerance = TimeSpan.FromMilliseconds(3);

        foreach (var group in groups)
        {
            var targetFrames = group
                .OrderBy(x => x.Ts)
                .ToList();

            if (targetFrames.Count == 0)
                continue;

            var targetAngles = hasPhase
                ? ExtractPhaseSeries(targetFrames, phase!)
                : CalculateSequenceSeries(
                    targetFrames,
                    sequence!);

            if (targetAngles.Count == 0)
                continue;

            var differences =
                ComputeAngleDifferenceWithFallback(
                    targetAngles,
                    refAngles,
                    tolerance);

            if (differences.Count == 0)
                continue;

            foreach (var point in differences)
            {
                cachePoints.Add((
                    group.Key,
                    point.ts,
                    point.difDeg));
            }

            if (!buildFrontSeries)
                continue;

            var first = targetFrames[0];

            series.Add(new
            {
                pmu = group.Key,
                pdc = first.PdcName,
                reference = referencePmu,
                kind,
                mode = hasPhase
                    ? "phase"
                    : "sequence",
                phase,
                seq = sequence,
                unit = "deg",

                // O repository já entregou o orçamento do preview.
                points = differences
                    .Select(p => new object[]
                    {
                        p.ts,
                        p.difDeg
                    })
                    .ToList()
            });
        }

        if (cachePoints.Count == 0)
            return AngleProjection.Empty;

        return new AngleProjection(
            series,
            cachePoints,
            cachePoints.Min(x => x.ts),
            cachePoints.Max(x => x.ts));
    }

    private static List<(DateTime ts, double angDeg)>
        ExtractPhaseSeries(
            IEnumerable<AngleFrameRow> frames,
            string phase)
    {
        var p = phase.Trim().ToUpperInvariant();

        return frames
            .Select(frame =>
            {
                double? angle = p switch
                {
                    "A" => frame.AAng,
                    "B" => frame.BAng,
                    "C" => frame.CAng,
                    _ => null
                };

                return (frame.Ts, angle);
            })
            .Where(x => x.angle.HasValue)
            .Select(x => (
                ts: x.Ts,
                angDeg: x.angle!.Value))
            .OrderBy(x => x.ts)
            .ToList();
    }

    /// <summary>
    /// Mesma transformação de componentes simétricas do handler anterior,
    /// agora aplicada diretamente à linha Wide, onde A/B/C já compartilham
    /// o mesmo timestamp físico.
    /// </summary>
    private static List<(DateTime ts, double angDeg)>
        CalculateSequenceSeries(
            IEnumerable<AngleFrameRow> frames,
            string sequence)
    {
        const double Deg2Rad = Math.PI / 180.0;
        const double Rad2Deg = 180.0 / Math.PI;

        var a = Complex.FromPolarCoordinates(
            1.0,
            120.0 * Deg2Rad);

        var a2 = Complex.FromPolarCoordinates(
            1.0,
            240.0 * Deg2Rad);

        var result = new List<(DateTime ts, double angDeg)>();

        foreach (var frame in frames.OrderBy(x => x.Ts))
        {
            if (!frame.AMod.HasValue ||
                !frame.AAng.HasValue ||
                !frame.BMod.HasValue ||
                !frame.BAng.HasValue ||
                !frame.CMod.HasValue ||
                !frame.CAng.HasValue)
            {
                continue;
            }

            var va = Complex.FromPolarCoordinates(
                frame.AMod.Value,
                frame.AAng.Value * Deg2Rad);

            var vb = Complex.FromPolarCoordinates(
                frame.BMod.Value,
                frame.BAng.Value * Deg2Rad);

            var vc = Complex.FromPolarCoordinates(
                frame.CMod.Value,
                frame.CAng.Value * Deg2Rad);

            var vSeq = sequence switch
            {
                "pos" => (va + a * vb + a2 * vc) / 3.0,
                "neg" => (va + a2 * vb + a * vc) / 3.0,
                "zero" => (va + vb + vc) / 3.0,
                _ => throw new ArgumentException(
                    "seq deve ser: pos | neg | zero")
            };

            result.Add((
                frame.Ts,
                vSeq.Phase * Rad2Deg));
        }

        return result;
    }

    /// <summary>
    /// Mantém tolerância de 3 ms e o conceito de fallback do handler antigo,
    /// mas não cria uma grade temporal artificial: o timestamp de saída é
    /// sempre o timestamp real da série medida.
    /// </summary>
    private static List<(DateTime ts, double difDeg)>
        ComputeAngleDifferenceWithFallback(
            IReadOnlyList<(DateTime ts, double angDeg)> measured,
            IReadOnlyList<(DateTime ts, double angDeg)> reference,
            TimeSpan tolerance)
    {
        if (measured.Count == 0 ||
            reference.Count == 0)
        {
            return new List<(DateTime ts, double difDeg)>();
        }

        // Primeiro identifica, para cada timestamp REAL da PMU medida,
        // se existe referência dentro da mesma tolerância de 3 ms.
        var candidates =
            new List<(DateTime ts, double? difDeg)>(
                measured.Count);

        var referenceIndex = 0;

        foreach (var measuredPoint in measured)
        {
            while (referenceIndex < reference.Count &&
                   reference[referenceIndex].ts <
                   measuredPoint.ts - tolerance)
            {
                referenceIndex++;
            }

            var bestIndex = -1;
            var bestDelta = long.MaxValue;

            for (var candidateIndex = referenceIndex;
                 candidateIndex < reference.Count;
                 candidateIndex++)
            {
                var candidate = reference[candidateIndex];

                if (candidate.ts >
                    measuredPoint.ts + tolerance)
                {
                    break;
                }

                var delta = Math.Abs(
                    (candidate.ts -
                     measuredPoint.ts).Ticks);

                if (delta < bestDelta)
                {
                    bestDelta = delta;
                    bestIndex = candidateIndex;
                }
            }

            double? difference = null;

            if (bestIndex >= 0)
            {
                difference = Wrap180(
                    measuredPoint.angDeg -
                    reference[bestIndex].angDeg);

                referenceIndex = bestIndex + 1;
            }

            candidates.Add((
                measuredPoint.ts,
                difference));
        }

        var firstValid = candidates
            .FirstOrDefault(x => x.difDeg.HasValue);

        if (!firstValid.difDeg.HasValue)
        {
            return new List<(DateTime ts, double difDeg)>();
        }

        // Preserva o comportamento de fallback do handler anterior:
        // antes/depois de uma lacuna, repete o último resultado válido.
        // A diferença é apenas temporal: não inventamos uma grade de
        // timestamps; usamos exclusivamente os timestamps reais medidos.
        var output =
            new List<(DateTime ts, double difDeg)>(
                candidates.Count);

        var lastDifference =
            firstValid.difDeg.Value;

        foreach (var candidate in candidates)
        {
            if (candidate.difDeg.HasValue)
            {
                lastDifference =
                    candidate.difDeg.Value;
            }

            output.Add((
                candidate.ts,
                lastDifference));
        }

        return output;
    }

    private static (
        bool isValid,
        string? errorMessage)
        ValidateInput(
            AngleDiffQuery query)
    {
        if (query.RunId == Guid.Empty)
            return (false, "run_id é obrigatório.");

        if (string.IsNullOrWhiteSpace(query.Kind))
            return (
                false,
                "kind é obrigatório (voltage|current).");

        var kind = query.Kind
            .Trim()
            .ToLowerInvariant();

        if (kind is not ("voltage" or "current"))
            return (
                false,
                "kind deve ser 'voltage' ou 'current'.");

        if (string.IsNullOrWhiteSpace(query.Reference))
            return (
                false,
                "ref é obrigatório (id_name da PMU referência).");

        var hasPhase =
            !string.IsNullOrWhiteSpace(query.Phase);

        var hasSequence =
            !string.IsNullOrWhiteSpace(query.Sequence);

        if (hasPhase == hasSequence)
        {
            return (
                false,
                "informe exatamente um dos parâmetros: phase (A|B|C) OU seq (pos|neg|zero).");
        }

        if (hasPhase)
        {
            var phase = query.Phase!
                .Trim()
                .ToUpperInvariant();

            if (phase is not ("A" or "B" or "C"))
                return (
                    false,
                    "phase deve ser A, B ou C.");
        }
        else if (NormalizeSeq(query.Sequence!) == "")
        {
            return (
                false,
                "seq inválida. Use pos|neg|zero (ou seq+|seq-|seq0).");
        }

        return (true, null);
    }

    private static double Wrap180(
        double angle)
    {
        angle %= 360.0;

        if (angle > 180.0)
            angle -= 360.0;

        if (angle < -180.0)
            angle += 360.0;

        return angle;
    }

    private static string NormalizeSeq(
        string sequence)
    {
        return sequence.Trim().ToLowerInvariant() switch
        {
            "pos" or "seq+" or "1" => "pos",
            "neg" or "seq-" or "2" => "neg",
            "zero" or "seq0" or "0" => "zero",
            _ => ""
        };
    }

    private sealed record AngleProjection(
        List<object> Series,
        List<(string pmuId, DateTime ts, double value)> CachePoints,
        DateTime WindowFrom,
        DateTime WindowTo)
    {
        public static readonly AngleProjection Empty =
            new(
                new List<object>(),
                new List<(string pmuId, DateTime ts, double value)>(),
                default,
                default);
    }
}
