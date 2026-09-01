using System.Globalization;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Contracts;

public interface IPlotMetaBuilder
{
    PlotMetaDto Build(
        WindowQuery w,
        RunContext ctx,
        MeasurementsQuery meas);
}

public sealed class PlotMetaBuilder : IPlotMetaBuilder
{
    public PlotMetaDto Build(
        WindowQuery w,
        RunContext ctx,
        MeasurementsQuery meas)
    {
        var title = BuildTitle(ctx, meas);
        var xLabel = BuildXLabel(w, ctx);
        var yLabel = BuildYLabel(meas);

        return new PlotMetaDto(
            title,
            xLabel,
            yLabel);
    }

    // ============================================================
    // EIXO X
    // ============================================================

    private static string BuildXLabel(
        WindowQuery w,
        RunContext ctx)
    {
        var from = w.FromUtc ?? ctx.FromUtc;
        var to = w.ToUtc ?? ctx.ToUtc;

        if (from.Date == to.Date)
        {
            var diaStr = from.ToString(
                "dd/MM/yyyy",
                CultureInfo.InvariantCulture);

            return $"Tempo (UTC) - Data {diaStr}";
        }

        return "Tempo (UTC)";
    }

    // ============================================================
    // EIXO Y
    // ============================================================

    private static string BuildYLabel(
        MeasurementsQuery meas)
    {
        var quantity = Norm(meas.Quantity);
        var component = Norm(meas.Component);
        var unit = Norm(meas.Unit);
        var phaseMode = ResolvePhaseMode(meas);

        // Diferença angular
        if (IsAngle(component))
            return "Diferença Angular (Graus)";

        // THD
        if (component == "thd")
            return "Distorção Harmônica (%)";

        // Variação de frequência
        if (
            component == "dfreq" ||
            quantity == "dfreq")
        {
            return "Variação de Frequência (Hz/s)";
        }

        // Frequência
        if (
            quantity == "frequency" ||
            component == "freq")
        {
            return "Frequência (Hz)";
        }

        // Desbalanço / VIMB
        if (
            phaseMode == PhaseMode.Deseq ||
            component is "unbalance" or "ratio")
        {
            return "Desbalanço de Tensão";
        }

        // Tensão
        if (quantity == "voltage")
        {
            return unit == "pu"
                ? "Tensão (pu)"
                : "Tensão (V)";
        }

        // Corrente
        if (quantity == "current")
        {
            return unit == "pu"
                ? "Corrente (pu)"
                : "Corrente (A)";
        }

        // Potência ativa
        if (quantity == "p_active")
            return "Potência (MW)";

        // Potência reativa
        if (quantity == "p_reactive")
            return "Potência (Mvar)";

        // CFDS
        if (
            quantity == "digital" ||
            component is "dig" or "cfds")
        {
            return
                "Sinal Digital de Falha de Comutação (Binário)";
        }

        return quantity switch
        {
            "voltage" => "Tensão",
            "current" => "Corrente",
            "p_active" => "Potência",
            "p_reactive" => "Potência",
            "frequency" => "Frequência",
            "dfreq" => "Variação de Frequência",
            "digital" =>
                "Sinal Digital de Falha de Comutação",
            _ => "Grandeza"
        };
    }

    // ============================================================
    // TÍTULO
    // ============================================================

    private static string BuildTitle(
        RunContext ctx,
        MeasurementsQuery meas)
    {
        var quantity = Norm(meas.Quantity);
        var component = Norm(meas.Component);
        var phaseMode = ResolvePhaseMode(meas);

        var terminal = meas.PmuNames?
            .FirstOrDefault()?
            .Trim();

        var labelGrandeza = quantity switch
        {
            "voltage" => "Tensão",
            "current" => "Corrente",
            "p_active" => "Potência Ativa",
            "p_reactive" => "Potência Reativa",
            "frequency" => "Frequência",
            "dfreq" => "Variação de Frequência",
            "digital" =>
                "Sinal Digital de Falha de Comutação",
            _ => "Grandeza"
        };

        var isDigital =
            quantity == "digital" ||
            component is "dig" or "cfds";

        var resSuffix =
            BuildResolutionSuffix(ctx, isDigital);

        // ========================================================
        // VARIAÇÃO DE FREQUÊNCIA
        // ========================================================

        if (
            component == "dfreq" ||
            quantity == "dfreq")
        {
            return
                "Variação de Frequência" +
                resSuffix;
        }

        // ========================================================
        // FREQUÊNCIA
        // ========================================================

        if (
            quantity == "frequency" ||
            component == "freq")
        {
            return
                "Frequência" +
                resSuffix;
        }

        // ========================================================
        // POTÊNCIA ATIVA
        // ========================================================

        if (quantity == "p_active")
        {
            return
                "Potência Ativa" +
                resSuffix;
        }

        // ========================================================
        // POTÊNCIA REATIVA
        // ========================================================

        if (quantity == "p_reactive")
        {
            return
                "Potência Reativa" +
                resSuffix;
        }

        // ========================================================
        // CFDS
        // ========================================================

        if (isDigital)
        {
            return
                "Sinal Digital de Falha de Comutação" +
                resSuffix;
        }

        // ========================================================
        // THD
        // ========================================================

        if (component == "thd")
        {
            var baseTitle =
                $"Distorção Harmônica Total da {labelGrandeza}";

            // THD por fase
            if (phaseMode == PhaseMode.Single)
            {
                var domain =
                    GetDomainLabel(meas, phaseMode);

                if (!string.IsNullOrWhiteSpace(domain))
                {
                    return
                        $"{baseTitle} - {domain}" +
                        resSuffix;
                }
            }

            // THD trifásico
            if (IsThreePhase(phaseMode))
            {
                if (!string.IsNullOrWhiteSpace(terminal))
                {
                    return
                        $"{baseTitle} - {terminal}" +
                        resSuffix;
                }
            }

            return baseTitle + resSuffix;
        }

        // ========================================================
        // DESBALANÇO / VIMB
        // ========================================================

        if (
            phaseMode == PhaseMode.Deseq ||
            component is "unbalance" or "ratio")
        {
            return
                "Desbalanço de Tensão" +
                resSuffix;
        }

        // ========================================================
        // DIFERENÇA ANGULAR
        // ========================================================

        if (IsAngle(component))
        {
            var title =
                $"Diferença Angular da {labelGrandeza}";

            var domain =
                GetDomainLabel(meas, phaseMode);

            if (!string.IsNullOrWhiteSpace(domain))
            {
                title += $" - {domain}";
            }

            if (!string.IsNullOrWhiteSpace(
                    meas.ReferenceTerminal))
            {
                title +=
                    $" - Ref.: {meas.ReferenceTerminal.Trim()}";
            }

            return title + resSuffix;
        }

        // ========================================================
        // SEQUÊNCIA
        //
        // IMPORTANTE:
        // fica antes do tratamento trifásico/fallback.
        // ========================================================

        if (
            phaseMode is
                PhaseMode.SeqPos or
                PhaseMode.SeqNeg or
                PhaseMode.SeqZero)
        {
            var seqLabel =
                GetSequenceLabel(phaseMode);

            return
                $"Módulo da {labelGrandeza} - {seqLabel}" +
                resSuffix;
        }

        // ========================================================
        // TRIFÁSICO
        // ========================================================

        if (IsThreePhase(phaseMode))
        {
            var title =
                $"Módulo da {labelGrandeza}";

            if (!string.IsNullOrWhiteSpace(terminal))
            {
                title += $" - {terminal}";
            }

            return title + resSuffix;
        }

        // ========================================================
        // MÓDULO POR FASE
        // ========================================================

        if (
            IsMagnitude(component) &&
            phaseMode == PhaseMode.Single)
        {
            var title =
                $"Módulo da {labelGrandeza}";

            var domain =
                GetDomainLabel(meas, phaseMode);

            if (!string.IsNullOrWhiteSpace(domain))
            {
                title += $" - {domain}";
            }

            return title + resSuffix;
        }

        // ========================================================
        // FALLBACK POR FASE
        //
        // Mesmo que algum handler não informe "mag",
        // se é tensão/corrente + Single sabemos que é por fase.
        // ========================================================

        if (
            phaseMode == PhaseMode.Single &&
            quantity is "voltage" or "current")
        {
            var title =
                $"Módulo da {labelGrandeza}";

            var domain =
                GetDomainLabel(meas, phaseMode);

            if (!string.IsNullOrWhiteSpace(domain))
            {
                title += $" - {domain}";
            }

            return title + resSuffix;
        }

        // ========================================================
        // FALLBACK
        // ========================================================

        if (labelGrandeza != "Grandeza")
            return labelGrandeza + resSuffix;

        return "Gráfico" + resSuffix;
    }

    // ============================================================
    // RESOLUÇÃO DO PHASE MODE
    // ============================================================

    private static PhaseMode ResolvePhaseMode(
        MeasurementsQuery meas)
    {
        var phase = Norm(meas.Phase);
        var component = Norm(meas.Component);

        // Se o handler já informou corretamente,
        // respeitamos primeiro o PhaseMode explícito.
        if (
            meas.PhaseMode is
                PhaseMode.Single or
                PhaseMode.ABC or
                PhaseMode.ThreePhase or
                PhaseMode.SeqPos or
                PhaseMode.SeqNeg or
                PhaseMode.SeqZero or
                PhaseMode.Deseq)
        {
            return meas.PhaseMode;
        }

        // Fallback para handlers/cache que codificam
        // sequência dentro de Phase.
        if (
            phase is
                "pos" or
                "positive" or
                "seq+")
        {
            return PhaseMode.SeqPos;
        }

        if (
            phase is
                "neg" or
                "negative" or
                "seq-")
        {
            return PhaseMode.SeqNeg;
        }

        if (
            phase is
                "zero" or
                "seq0")
        {
            return PhaseMode.SeqZero;
        }

        // Fase convencional
        if (phase is "a" or "b" or "c")
            return PhaseMode.Single;

        // VIMB
        if (component is "unbalance" or "ratio")
            return PhaseMode.Deseq;

        return meas.PhaseMode;
    }

    // ============================================================
    // DOMÍNIO
    // ============================================================

    private static string GetDomainLabel(
        MeasurementsQuery meas,
        PhaseMode phaseMode)
    {
        var phase = Norm(meas.Phase);

        return phaseMode switch
        {
            PhaseMode.Single when phase == "a"
                => "Fase A",

            PhaseMode.Single when phase == "b"
                => "Fase B",

            PhaseMode.Single when phase == "c"
                => "Fase C",

            PhaseMode.SeqPos
                => "Sequência Positiva",

            PhaseMode.SeqNeg
                => "Sequência Negativa",

            PhaseMode.SeqZero
                => "Sequência Zero",

            _ => ""
        };
    }

    private static string GetSequenceLabel(
        PhaseMode phaseMode)
    {
        return phaseMode switch
        {
            PhaseMode.SeqPos
                => "Sequência Positiva",

            PhaseMode.SeqNeg
                => "Sequência Negativa",

            PhaseMode.SeqZero
                => "Sequência Zero",

            _ => ""
        };
    }

    // ============================================================
    // TIPOS
    // ============================================================

    private static bool IsThreePhase(
        PhaseMode phaseMode)
    {
        return phaseMode is
            PhaseMode.ABC or
            PhaseMode.ThreePhase;
    }

    private static bool IsAngle(
        string component)
    {
        return component is
            "angle" or
            "ang" or
            "angle_diff_phase" or
            "angle_diff_sequence";
    }

    private static bool IsMagnitude(
        string component)
    {
        return component is
            "mag" or
            "mod" or
            "magnitude" or
            "seq";
    }

    // ============================================================
    // TAXA
    // ============================================================

    private static string BuildResolutionSuffix(
        RunContext ctx,
        bool isDigital)
    {
        var rate = ctx.SelectRate;

        if (rate <= 0)
            return "";

        var unit = (rate > 1 ? "fasores" : "fasor");

        return $" - {rate} {unit}/s";
    }

    // ============================================================
    // NORMALIZAÇÃO
    // ============================================================

    private static string Norm(string? value)
    {
        return (value ?? "")
            .Trim()
            .ToLowerInvariant();
    }
}