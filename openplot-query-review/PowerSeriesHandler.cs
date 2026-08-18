using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Data.Dtos;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para cálculo de potência ativa/reativa.
/// Recebe V/I em fasores (MAG+ANG) do MeasurementsRepository e calcula P/Q
/// por fase ou total.
///
/// O preview já chega reduzido pelo TimescaleDB em QueryAbcMagAngAsync.
/// O processamento/cache completo usa maxPoints=null e continua trabalhando
/// com a massa integral.
/// </summary>
public sealed class PowerSeriesHandler
{
    private readonly IDbConnectionFactory _dbFactory;
    private readonly IMeasurementsRepository _measurementsRepository;
    private readonly IRunContextRepository _runRepository;
    private readonly ILogger<PowerSeriesHandler> _logger;
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IPlotMetaBuilder _metaBuilder;
    private readonly IPmuQueryHelper _pmuHelper;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly IUiMenuService _uiMenus;

    public PowerSeriesHandler(
        IRunContextRepository runRepository,
        IDbConnectionFactory dbFactory,
        IMeasurementsRepository measurementsRepository,
        IAnalysisCacheRepository cacheRepo,
        IPlotMetaBuilder metaBuilder,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly,
        IUiMenuService uiMenus,
        ILogger<PowerSeriesHandler> logger)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _dbFactory = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory));
        _measurementsRepository = measurementsRepository ?? throw new ArgumentNullException(nameof(measurementsRepository));
        _cacheRepo = cacheRepo ?? throw new ArgumentNullException(nameof(cacheRepo));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _pmuHelper = pmuHelper ?? throw new ArgumentNullException(nameof(pmuHelper));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IResult> HandleAsync(
        PowerPlotQuery query,
        WindowQuery window,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var validation = ValidatePowerQuery(query);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        var processingWatch = Stopwatch.StartNew();

        var which = (query.Which ?? "active").Trim().ToLowerInvariant();
        var quantityKey = which == "active" ? "p_active" : "p_reactive";
        var unit = (query.Unit ?? "raw").Trim().ToLowerInvariant();
        var maxPts = Math.Max(query.ResolveMaxPoints(@default: 100), 100);

        _logger.LogInformation(
            "[PROCESS][Power][START] runId={RunId} which={Which} tri={Tri} total={Total} phase={Phase}",
            query.RunId,
            which,
            query.Tri,
            query.Total,
            query.Phase);

        DateTime? fromUtc = window.FromUtc;
        DateTime? toUtc = window.ToUtc;

        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepository.ResolveAsync(
            query.RunId,
            fromUtc,
            toUtc,
            ct);

        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var pmuList = _pmuHelper.Normalize(query.Pmu).ToList();

        var tri = query.Tri ?? false;
        var total = query.Total ?? false;
        var phase = !tri && !total
            ? query.Phase?.Trim().ToUpperInvariant()
            : null;

        var phaseFilter = !tri && !total ? phase : null;
        IReadOnlyList<string>? pmuFilter = pmuList.Count > 0 ? pmuList : null;

        // maxPoints != null => preview.
        // O MeasurementsRepository faz time_bucket/first no frame Wide antes
        // de expandir ABC MAG+ANG em SignalId.
        int? frontMaxPoints = query.MaxPointsIsAll ? null : maxPts;

        var voltageRows = await _measurementsRepository.QueryAbcMagAngAsync(
            ctx,
            kind: "voltage",
            pmuNames: pmuFilter,
            fromUtc: fromUtc,
            toUtc: toUtc,
            ct,
            frontMaxPoints);

        var currentRows = await _measurementsRepository.QueryAbcMagAngAsync(
            ctx,
            kind: "current",
            pmuNames: pmuFilter,
            fromUtc: fromUtc,
            toUtc: toUtc,
            ct,
            frontMaxPoints);

        var rows = BuildPowerRows(
            voltageRows,
            currentRows,
            tri,
            total,
            phaseFilter);

        if (rows.Count == 0)
            return Results.NotFound(
                "Nada encontrado para esse run/filtro no intervalo solicitado.");

        // O input já está reduzido no banco quando esta é uma consulta de
        // preview. Portanto NÃO há segundo downsampling em C#.
        var projection = BuildPowerProjection(
            rows,
            tri,
            total,
            phase,
            which,
            unit,
            buildFrontSeries: true);

        var seriesOut = projection.series;
        var cachePoints = projection.cachePoints;

        if (seriesOut.Count == 0)
            return Results.BadRequest(
                "Nenhuma PMU pôde ser processada (faltam sinais MAG/ANG de V/I ou alinhamento falhou).");

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo = toUtc ?? rows.Max(r => r.Ts);
        var unitDisplay = unit == "mw"
            ? (which == "active" ? "MW" : "MVAr")
            : "raw";

        // Salva imediatamente o preview para que o cache_id retornado seja
        // utilizável. Em seguida o job em background substitui o conteúdo
        // pelo processamento integral.
        var cacheSeries = cachePoints
            .GroupBy(x => x.pmuId)
            .Select(g => _seriesAssembly.BuildCacheSeries(
                signalId: 0,
                pdcPmuId: 0,
                idName: g.Key,
                pdcName: ctx.PdcName,
                referenceTerminal: null,
                unit: unitDisplay,
                phase: null,
                quantity: which,
                component: "power",
                points: g.Select(x => (x.ts, x.value))))
            .ToList();

        var cachePayload = _seriesAssembly.BuildCachePayload(
            windowFrom,
            windowTo,
            ctx.SelectRate ?? 0,
            cacheSeries);

        var cacheId = Guid.NewGuid();

        await _cacheRepo.SaveAsync(
            cacheId,
            query.RunId,
            cachePayload,
            ct);

        // Cache analítico completo:
        // maxPoints=null => sem downsampling no repository.
        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;

            _logger.LogInformation(
                "[BYRUN][Power][CACHE-BG][START] runId={RunId} cacheId={CacheId}",
                query.RunId,
                cacheId);

            try
            {
                var fullVoltageRows = await _measurementsRepository.QueryAbcMagAngAsync(
                    ctx,
                    kind: "voltage",
                    pmuNames: pmuFilter,
                    fromUtc: fromUtc,
                    toUtc: toUtc,
                    CancellationToken.None,
                    maxPoints: null);

                var fullCurrentRows = await _measurementsRepository.QueryAbcMagAngAsync(
                    ctx,
                    kind: "current",
                    pmuNames: pmuFilter,
                    fromUtc: fromUtc,
                    toUtc: toUtc,
                    CancellationToken.None,
                    maxPoints: null);

                var fullRows = BuildPowerRows(
                    fullVoltageRows,
                    fullCurrentRows,
                    tri,
                    total,
                    phaseFilter);

                fullRowsCount = fullRows.Count;
                if (fullRowsCount == 0)
                    return;

                var fullProjection = BuildPowerProjection(
                    fullRows,
                    tri,
                    total,
                    phase,
                    which,
                    unit,
                    buildFrontSeries: false);

                if (fullProjection.cachePoints.Count == 0)
                    return;

                var fullWindowFrom = fromUtc ?? fullRows.Min(r => r.Ts);
                var fullWindowTo = toUtc ?? fullRows.Max(r => r.Ts);

                var cacheSeriesFull = fullProjection.cachePoints
                    .GroupBy(x => x.pmuId)
                    .Select(g => _seriesAssembly.BuildCacheSeries(
                        signalId: 0,
                        pdcPmuId: 0,
                        idName: g.Key,
                        pdcName: ctx.PdcName,
                        referenceTerminal: null,
                        unit: unitDisplay,
                        phase: null,
                        quantity: which,
                        component: "power",
                        points: g.Select(x => (x.ts, x.value))))
                    .ToList();

                var cachePayloadFull = _seriesAssembly.BuildCachePayload(
                    fullWindowFrom,
                    fullWindowTo,
                    ctx.SelectRate ?? 0,
                    cacheSeriesFull);

                await _cacheRepo.SaveAsync(
                    cacheId,
                    query.RunId,
                    cachePayloadFull,
                    CancellationToken.None);

                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Falha ao persistir cache assíncrono de power/by-run. runId={RunId}",
                    query.RunId);
            }
            finally
            {
                bgWatch.Stop();

                _logger.LogInformation(
                    "[BYRUN][Power][CACHE-BG][END] runId={RunId} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}",
                    query.RunId,
                    cacheId,
                    bgWatch.ElapsedMilliseconds,
                    fullRowsCount,
                    persisted);
            }
        });

        var meas = new MeasurementsQuery(
            Quantity: quantityKey,
            Component: "power",
            PhaseMode: PhaseMode.Any,
            Unit: unitDisplay);

        var plotMeta = _metaBuilder.Build(window, ctx, meas);

        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            UiMenuContext.FromCache(cachePayload));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(
                query.RunId,
                windowFrom,
                windowTo,
                seriesOut,
                plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, seriesOut.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unitDisplay,
                ["type"] = which,
                ["tri"] = tri,
                ["total"] = total,
                ["phase"] = phase
            })
            .Build();

        processingWatch.Stop();

        _logger.LogInformation(
            "[PROCESS][Power][END] runId={RunId} elapsedMs={ElapsedMs} series={SeriesCount}",
            query.RunId,
            processingWatch.ElapsedMilliseconds,
            seriesOut.Count);

        return Results.Ok(response);
    }

    private static (bool isValid, string? errorMessage) ValidatePowerQuery(
        PowerPlotQuery query)
    {
        if (query.RunId == Guid.Empty)
            return (false, "run_id é obrigatório.");

        var which = (query.Which ?? "active").Trim().ToLowerInvariant();
        if (which is not ("active" or "reactive"))
            return (false, "which deve ser 'active' ou 'reactive'.");

        var unit = (query.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "mw"))
            return (false, "unit deve ser 'raw' ou 'mw'.");

        var tri = query.Tri ?? false;
        var total = query.Total ?? false;

        if (tri && total)
            return (false, "tri=true e total=true são mutuamente exclusivos.");

        if (tri && (query.Pmu?.Length ?? 0) != 1)
            return (false, "tri=true exige exatamente 1 pmu (id_name).");

        if (!tri && !total && string.IsNullOrWhiteSpace(query.Phase))
            return (false, "phase é obrigatório quando tri=false e total=false.");

        if (!tri && !total)
        {
            var phase = query.Phase?.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }

        return (true, null);
    }

    private static List<PowerRow> BuildPowerRows(
        IReadOnlyList<PhasorAbcRow> voltageRows,
        IReadOnlyList<PhasorAbcRow> currentRows,
        bool tri,
        bool total,
        string? phaseFilter)
    {
        return voltageRows
            .Select(r => (Row: r, Quantity: "voltage"))
            .Concat(currentRows.Select(r => (Row: r, Quantity: "current")))
            .Where(x =>
                tri ||
                total ||
                string.Equals(
                    x.Row.Phase,
                    phaseFilter,
                    StringComparison.OrdinalIgnoreCase))
            .Select(x => new PowerRow
            {
                Signal_Id = x.Row.SignalId,
                Pdc_Pmu_Id = x.Row.PdcPmuId,
                Phase = x.Row.Phase,
                Component = x.Row.Component,
                Quantity = x.Quantity,
                Id_Name = x.Row.IdName,
                Pdc_Name = x.Row.PdcName,
                Ts = x.Row.Ts,
                Value = x.Row.Value
            })
            .ToList();
    }

    private static (
        List<object> series,
        List<(string pmuId, DateTime ts, double value)> cachePoints)
        BuildPowerProjection(
            IReadOnlyList<PowerRow> rows,
            bool tri,
            bool total,
            string? phase,
            string which,
            string unit,
            bool buildFrontSeries)
    {
        var tol = TimeSpan.FromMilliseconds(3);
        var seriesOut = new List<object>();
        var cachePoints =
            new List<(string pmuId, DateTime ts, double value)>();

        var unitDisplay = unit == "mw"
            ? (which == "active" ? "MW" : "MVAr")
            : "raw";

        // Mantém a mesma escala usada pelo handler anterior.
        // A semântica de unidade não é alterada nesta mudança.
        const double scale = 1e-6;

        foreach (var pmuGroup in rows.GroupBy(r => r.Id_Name))
        {
            var d = new Dictionary<string, List<(DateTime ts, double v)>>();

            foreach (var r in pmuGroup)
            {
                var qty = (r.Quantity ?? string.Empty).ToLowerInvariant();
                var phs = (r.Phase ?? string.Empty).ToUpperInvariant();
                var cmp = (r.Component ?? string.Empty).ToUpperInvariant();

                if (qty is not ("voltage" or "v" or "current" or "i"))
                    continue;

                if (phs is not ("A" or "B" or "C"))
                    continue;

                if (cmp is not ("MAG" or "ANG"))
                    continue;

                var qn = qty switch
                {
                    "v" => "voltage",
                    "i" => "current",
                    _ => qty
                };

                if (!d.TryGetValue(
                        $"{qn}_{phs}_{cmp}",
                        out var list))
                {
                    list = new List<(DateTime ts, double v)>();
                    d[$"{qn}_{phs}_{cmp}"] = list;
                }

                list.Add((r.Ts, r.Value));
            }

            bool Need(string key) =>
                d.TryGetValue(key, out var points) &&
                points.Count > 0;

            List<(DateTime ts, double val)> MakePhase(string phs)
            {
                var vMagK = $"voltage_{phs}_MAG";
                var vAngK = $"voltage_{phs}_ANG";
                var iMagK = $"current_{phs}_MAG";
                var iAngK = $"current_{phs}_ANG";

                if (!Need(vMagK) ||
                    !Need(vAngK) ||
                    !Need(iMagK) ||
                    !Need(iAngK))
                {
                    return new List<(DateTime ts, double val)>();
                }

                return ComputePower1Phase(
                    d[vMagK],
                    d[vAngK],
                    d[iMagK],
                    d[iAngK],
                    tol,
                    which);
            }

            var any = pmuGroup.First();

            if (tri)
            {
                foreach (var phs in new[] { "A", "B", "C" })
                {
                    var pts = MakePhase(phs);
                    if (pts.Count == 0)
                        continue;

                    var scaled = pts
                        .Select(x => (x.ts, val: x.val * scale))
                        .ToList();

                    foreach (var pt in scaled)
                        cachePoints.Add((any.Id_Name, pt.ts, pt.val));

                    if (!buildFrontSeries)
                        continue;

                    // Sem downsampling em C#: QueryAbcMagAngAsync já fez a
                    // redução no TimescaleDB para o preview.
                    seriesOut.Add(new
                    {
                        pmu = any.Id_Name,
                        pdc = any.Pdc_Name,
                        meta = new { phase = phs },
                        unit = unitDisplay,
                        points = scaled.Select(
                            p => new object[] { p.ts, p.val })
                    });
                }
            }
            else if (total)
            {
                var aPts = MakePhase("A");
                var bPts = MakePhase("B");
                var cPts = MakePhase("C");

                if (aPts.Count == 0 ||
                    bPts.Count == 0 ||
                    cPts.Count == 0)
                {
                    continue;
                }

                var sum = Sum3PhasePointwise(
                    aPts,
                    bPts,
                    cPts,
                    tol);

                if (sum.Count == 0)
                    continue;

                var scaled = sum
                    .Select(x => (x.ts, val: x.val * scale))
                    .ToList();

                foreach (var pt in scaled)
                    cachePoints.Add((any.Id_Name, pt.ts, pt.val));

                if (!buildFrontSeries)
                    continue;

                seriesOut.Add(new
                {
                    pmu = any.Id_Name,
                    pdc = any.Pdc_Name,
                    meta = new { total = true },
                    unit = unitDisplay,
                    points = scaled.Select(
                        p => new object[] { p.ts, p.val })
                });
            }
            else
            {
                var pts = MakePhase(phase!);
                if (pts.Count == 0)
                    continue;

                var scaled = pts
                    .Select(x => (x.ts, val: x.val * scale))
                    .ToList();

                foreach (var pt in scaled)
                    cachePoints.Add((any.Id_Name, pt.ts, pt.val));

                if (!buildFrontSeries)
                    continue;

                seriesOut.Add(new
                {
                    pmu = any.Id_Name,
                    pdc = any.Pdc_Name,
                    meta = new { phase },
                    unit = unitDisplay,
                    points = scaled.Select(
                        p => new object[] { p.ts, p.val })
                });
            }
        }

        return (seriesOut, cachePoints);
    }

    private static List<(DateTime ts, double val)> ComputePower1Phase(
        List<(DateTime ts, double v)> vMag,
        List<(DateTime ts, double v)> vAng,
        List<(DateTime ts, double v)> iMag,
        List<(DateTime ts, double v)> iAng,
        TimeSpan tol,
        string which)
    {
        vMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        vAng.Sort((a, b) => a.ts.CompareTo(b.ts));
        iMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        iAng.Sort((a, b) => a.ts.CompareTo(b.ts));

        var ivm = 0;
        var iva = 0;
        var iim = 0;
        var iia = 0;

        const double Deg2Rad = Math.PI / 180.0;

        static void Adv(
            ref int idx,
            List<(DateTime ts, double v)> list,
            DateTime t,
            TimeSpan tolerance)
        {
            while (idx < list.Count &&
                   list[idx].ts < t &&
                   (t - list[idx].ts) > tolerance)
            {
                idx++;
            }
        }

        static bool Near(
            List<(DateTime ts, double v)> list,
            int idx,
            DateTime t,
            TimeSpan tolerance) =>
            idx < list.Count &&
            Math.Abs((list[idx].ts - t).TotalMilliseconds)
                <= tolerance.TotalMilliseconds;

        var output = new List<(DateTime ts, double val)>();

        while (ivm < vMag.Count && iim < iMag.Count)
        {
            var t = vMag[ivm].ts;
            if (iMag[iim].ts > t)
                t = iMag[iim].ts;

            Adv(ref ivm, vMag, t, tol);
            Adv(ref iim, iMag, t, tol);

            if (ivm >= vMag.Count || iim >= iMag.Count)
                break;

            Adv(ref iva, vAng, t, tol);
            Adv(ref iia, iAng, t, tol);

            if (iva >= vAng.Count || iia >= iAng.Count)
                break;

            if (!Near(vMag, ivm, t, tol) ||
                !Near(iMag, iim, t, tol) ||
                !Near(vAng, iva, t, tol) ||
                !Near(iAng, iia, t, tol))
            {
                var min = vMag[ivm].ts < iMag[iim].ts
                    ? vMag[ivm].ts
                    : iMag[iim].ts;

                if (min == vMag[ivm].ts)
                    ivm++;
                else
                    iim++;

                continue;
            }

            var apparent = vMag[ivm].v * iMag[iim].v;
            var angleDiff =
                (vAng[iva].v - iAng[iia].v) * Deg2Rad;

            var value = which == "active"
                ? apparent * Math.Cos(angleDiff)
                : apparent * Math.Sin(angleDiff);

            output.Add((t, value));

            ivm++;
            iim++;
            iva++;
            iia++;
        }

        return output;
    }

    private static List<(DateTime ts, double val)> Sum3PhasePointwise(
        List<(DateTime ts, double val)> a,
        List<(DateTime ts, double val)> b,
        List<(DateTime ts, double val)> c,
        TimeSpan tol)
    {
        a.Sort((x, y) => x.ts.CompareTo(y.ts));
        b.Sort((x, y) => x.ts.CompareTo(y.ts));
        c.Sort((x, y) => x.ts.CompareTo(y.ts));

        var ia = 0;
        var ib = 0;
        var ic = 0;

        var output = new List<(DateTime ts, double val)>();

        while (ia < a.Count && ib < b.Count && ic < c.Count)
        {
            var t = a[ia].ts;
            if (b[ib].ts > t)
                t = b[ib].ts;
            if (c[ic].ts > t)
                t = c[ic].ts;

            while (ia < a.Count &&
                   a[ia].ts < t &&
                   (t - a[ia].ts) > tol)
            {
                ia++;
            }

            while (ib < b.Count &&
                   b[ib].ts < t &&
                   (t - b[ib].ts) > tol)
            {
                ib++;
            }

            while (ic < c.Count &&
                   c[ic].ts < t &&
                   (t - c[ic].ts) > tol)
            {
                ic++;
            }

            if (ia >= a.Count ||
                ib >= b.Count ||
                ic >= c.Count)
            {
                break;
            }

            if (Math.Abs((a[ia].ts - t).TotalMilliseconds)
                    > tol.TotalMilliseconds ||
                Math.Abs((b[ib].ts - t).TotalMilliseconds)
                    > tol.TotalMilliseconds ||
                Math.Abs((c[ic].ts - t).TotalMilliseconds)
                    > tol.TotalMilliseconds)
            {
                var min = a[ia].ts;

                if (b[ib].ts < min)
                    min = b[ib].ts;
                if (c[ic].ts < min)
                    min = c[ic].ts;

                if (min == a[ia].ts)
                    ia++;
                else if (min == b[ib].ts)
                    ib++;
                else
                    ic++;

                continue;
            }

            output.Add((
                t,
                a[ia].val + b[ib].val + c[ic].val));

            ia++;
            ib++;
            ic++;
        }

        return output;
    }
}