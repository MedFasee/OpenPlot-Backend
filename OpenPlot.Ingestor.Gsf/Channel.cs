using System;
using System.Collections.Generic;
using System.Text;

namespace OpenPlot.Ingestor.Gsf
{
    public class Channel
    {
        public static Channel VOLTAGE_A_MOD = new Channel(0, "VA_mod_(V)", ChannelPhase.PHASE_A, ChannelValueType.ABSOLUTE, ChannelQuantity.VOLTAGE);
        public static Channel VOLTAGE_A_ANG = new Channel(0, "VA_ang_(graus)", ChannelPhase.PHASE_A, ChannelValueType.ANGLE, ChannelQuantity.VOLTAGE);
        public static Channel VOLTAGE_B_MOD = new Channel(1, "VB_mod_(V)", ChannelPhase.PHASE_B, ChannelValueType.ABSOLUTE, ChannelQuantity.VOLTAGE);
        public static Channel VOLTAGE_B_ANG = new Channel(1, "VB_ang_(graus)", ChannelPhase.PHASE_B, ChannelValueType.ANGLE, ChannelQuantity.VOLTAGE);
        public static Channel VOLTAGE_C_MOD = new Channel(2, "VC_mod_(V)", ChannelPhase.PHASE_C, ChannelValueType.ABSOLUTE, ChannelQuantity.VOLTAGE);
        public static Channel VOLTAGE_C_ANG = new Channel(2, "VC_ang_(graus)", ChannelPhase.PHASE_C, ChannelValueType.ANGLE, ChannelQuantity.VOLTAGE);

        public static Channel CURRENT_A_MOD = new Channel(3, "IA_mod_(A)", ChannelPhase.PHASE_A, ChannelValueType.ABSOLUTE, ChannelQuantity.CURRENT);
        public static Channel CURRENT_A_ANG = new Channel(3, "IA_ang_(graus)", ChannelPhase.PHASE_A, ChannelValueType.ANGLE, ChannelQuantity.CURRENT);
        public static Channel CURRENT_B_MOD = new Channel(4, "IB_mod_(A)", ChannelPhase.PHASE_B, ChannelValueType.ABSOLUTE, ChannelQuantity.CURRENT);
        public static Channel CURRENT_B_ANG = new Channel(4, "IB_ang_(graus)", ChannelPhase.PHASE_B, ChannelValueType.ANGLE, ChannelQuantity.CURRENT);
        public static Channel CURRENT_C_MOD = new Channel(5, "IC_mod_(A)", ChannelPhase.PHASE_C, ChannelValueType.ABSOLUTE, ChannelQuantity.CURRENT);
        public static Channel CURRENT_C_ANG = new Channel(5, "IC_ang_(graus)", ChannelPhase.PHASE_C, ChannelValueType.ANGLE, ChannelQuantity.CURRENT);

        public static Channel VOLTAGE_POS_MOD = new Channel(0, "V_Seq_Pos_(V)", ChannelPhase.POS_SEQ, ChannelValueType.ABSOLUTE, ChannelQuantity.VOLTAGE);
        public static Channel VOLTAGE_POS_ANG = new Channel(0, "V_Seq_Pos_(graus)", ChannelPhase.POS_SEQ, ChannelValueType.ANGLE, ChannelQuantity.VOLTAGE);
        public static Channel CURRENT_POS_MOD = new Channel(0, "I_Seq_Pos_(A)", ChannelPhase.POS_SEQ, ChannelValueType.ABSOLUTE, ChannelQuantity.CURRENT);
        public static Channel CURRENT_POS_ANG = new Channel(0, "I_Seq_Pos_(graus)", ChannelPhase.POS_SEQ, ChannelValueType.ANGLE, ChannelQuantity.CURRENT);

        public static Channel VTHD_A = new Channel(0, "VTHDA_(%)", ChannelPhase.PHASE_A, ChannelValueType.VTHD, ChannelQuantity.ANALOG);
        public static Channel VTHD_B = new Channel(0, "VTHDB_(%)", ChannelPhase.PHASE_B, ChannelValueType.VTHD, ChannelQuantity.ANALOG);
        public static Channel VTHD_C = new Channel(0, "VTHDC_(%)", ChannelPhase.PHASE_C, ChannelValueType.VTHD, ChannelQuantity.ANALOG);

        public static Channel CTHD_A = new Channel(0, "CTHDA_(%)", ChannelPhase.PHASE_A, ChannelValueType.CTHD, ChannelQuantity.ANALOG);
        public static Channel CTHD_B = new Channel(0, "CTHDB_(%)", ChannelPhase.PHASE_B, ChannelValueType.CTHD, ChannelQuantity.ANALOG);
        public static Channel CTHD_C = new Channel(0, "CTHDC_(%)", ChannelPhase.PHASE_C, ChannelValueType.CTHD, ChannelQuantity.ANALOG);

        public static Channel VIMB = new Channel(0, "VIMB_(%)", ChannelPhase.NONE, ChannelValueType.VIMB, ChannelQuantity.ANALOG);
        public static Channel CIMB = new Channel(0, "CIMB_(%)", ChannelPhase.NONE, ChannelValueType.CIMB, ChannelQuantity.ANALOG);

        public static Channel CFDS = new Channel(0, "SDFC_(binário)", ChannelPhase.NONE, ChannelValueType.CFDS, ChannelQuantity.DIGITAL);

        public static Channel ACTIVE_POWER = new Channel(0, "Pot. Ativa (MW)", ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.ACTIV_PWR);
        public static Channel REACTIVE_POWER = new Channel(0, "Pot. Reativa (MVAr)", ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.REACT_PWR);
        public static Channel FREQ = new Channel(0, "Frequência", ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.FREQUENCY);
        public static Channel DFREQ = new Channel(0, "Delta_Freq", ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.DFREQ);
        public static Channel MISSING = new Channel(0, "Faltante", ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.MISSING);

        public static readonly Channel[] DEFAULT_CHANNELS = new Channel[] { VOLTAGE_A_MOD, VOLTAGE_A_ANG, VOLTAGE_B_MOD, VOLTAGE_B_ANG, VOLTAGE_C_MOD, VOLTAGE_C_ANG,
            CURRENT_A_MOD, CURRENT_A_ANG, CURRENT_B_MOD, CURRENT_B_ANG, CURRENT_C_MOD, CURRENT_C_ANG, VTHD_A, VTHD_B, VTHD_C, CTHD_A, CTHD_B, CTHD_C, VIMB, CIMB, CFDS, FREQ, DFREQ };

        public int Id { get; private set; }
        public string Name { get; private set; }
        public ChannelPhase Phase { get; private set; }
        public ChannelValueType Value { get; private set; }
        public ChannelQuantity Quantity { get; private set; }

        public Channel(int id, string name, ChannelPhase phase, ChannelValueType value, ChannelQuantity quantity)
        {
            Id = id;
            Name = name;
            Phase = phase;
            Value = value;
            Quantity = quantity;
        }

        public static ChannelPhase GetPhaseFromString(string phase)
        {
            if (string.IsNullOrEmpty(phase))
                return ChannelPhase.NONE;

            switch (phase.ToLower())
            {
                case ("a"):
                    return ChannelPhase.PHASE_A;
                case ("b"):
                    return ChannelPhase.PHASE_B;
                case ("c"):
                    return ChannelPhase.PHASE_C;
                case ("n"):
                    return ChannelPhase.NEUTRAL;
                default:
                    return ChannelPhase.NONE;
            }
        }

        public static ChannelValueType GetValueTypeFromString(string valueType)
        {
            switch (valueType == null ? "" : valueType.ToLower())
            {
                case ("absolute"):
                case ("abs"):
                case ("mod"):
                case ("module"):
                    return ChannelValueType.ABSOLUTE;
                case ("angle"):
                case ("ang"):
                    return ChannelValueType.ANGLE;
                default:
                    return ChannelValueType.NONE;
            }
        }

        public static ChannelQuantity GetQuantityFromString(string quantity)
        {
            switch (quantity.ToLower())
            {
                case ("freq"):
                case ("frequency"):
                    return ChannelQuantity.FREQUENCY;
                case ("dfreq"):
                    return ChannelQuantity.DFREQ;
                case ("voltage"):
                    return ChannelQuantity.VOLTAGE;
                case ("current"):
                    return ChannelQuantity.CURRENT;
                case ("analog"):
                    return ChannelQuantity.ANALOG;
                case ("digital"):
                    return ChannelQuantity.DIGITAL;
                default:
                    return ChannelQuantity.OTHER;
            }
        }

        public static string GetPhaseString(ChannelPhase phase)
        {
            switch (phase)
            {
                case ChannelPhase.PHASE_A:
                    return "A";
                case ChannelPhase.PHASE_B:
                    return "B";
                case ChannelPhase.PHASE_C:
                    return "C";
                case ChannelPhase.NEUTRAL:
                    return "N";
                case ChannelPhase.POS_SEQ:
                    return "+";
                case ChannelPhase.NONE:
                default:
                    return "";
            }
        }

        public override bool Equals(object obj)
        {
            return obj is Channel channel &&
                   Phase == channel.Phase &&
                   Value == channel.Value &&
                   Quantity == channel.Quantity;
        }

        public override int GetHashCode()
        {
            var hashCode = -1482258299;
            hashCode = hashCode * -1521134295 + Phase.GetHashCode();
            hashCode = hashCode * -1521134295 + Value.GetHashCode();
            hashCode = hashCode * -1521134295 + Quantity.GetHashCode();
            return hashCode;
        }
    }
}
