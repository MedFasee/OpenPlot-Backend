using OpenPlot.Ingestor.Gsf;
using OpenPlot.Ingestor.Gsf.Data;
using OpenPlot.Ingestor.Gsf.Repository;
using OpenPlot.Ingestor.Gsf.Utils;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class MeasurementMedFasee : Database, IMeasurementDb
    {
        private const int COMMAND_TIMEOUT = 100;

        private string Database { get; }
        private readonly string connectionString;

        public MeasurementMedFasee(string ip, int port, string user, string pass, string database)
            : base(ip, user, pass)
        {
            Database = database;

            // Uso de interpolação de string para clareza (assumindo C# 6.0 ou superior)
            connectionString =
                $"server={ip};" +
                $"Port={port};" +
                $"User ID={user};" +
                $"Password={pass};" +
                $"Database={database};" +
                "Connection Timeout=100";
        }

        // regra de downsample baseada em cont
        private static string GetRate(int dataRate, int equipmentRate)
        {
            if (dataRate <= 0 || equipmentRate <= 0)
            {
                return string.Empty;
            }

            // cont==0 quando dataRate == equipmentRate*1 (um ponto por ciclo do equipamento)
            if (equipmentRate / dataRate == equipmentRate)
            {
                return " and (cont=0)";
            }

            // passo por módulo quando há subamostragem
            if (equipmentRate / dataRate != 1)
            {
                // Uso de interpolação e formatação direta
                return $" and (cont%{(equipmentRate / dataRate):0.#}=0)";
            }

            return string.Empty;
        }

        // retorna apenas IDs de canais fasoriais (ABSOLUTE)
        private static string GetChannels(List<Channel> measurements)
        {
            var sb = new StringBuilder();
            string prefix = "";
            foreach (Channel channel in measurements)
            {
                if (channel.Value != ChannelValueType.ABSOLUTE) // magnitude/ângulo
                {
                    continue;
                }

                sb.Append(prefix);
                sb.Append(channel.Id); // <-- ID numérico do canal no MedFasee
                prefix = ",";
            }
            return sb.ToString();
        }

        // monta consulta diária; evita IN () quando não há canais fasoriais
        private static string BuildQuery(string id, DateTime start, DateTime finish, string channelsCsv, string rate)
        {
            var sb = new StringBuilder();
            string union = "";

            foreach (DateTime day in TimeUtils.EachDay(start, finish))
            {
                sb.Append(union);
                sb.Append("SELECT DISTINCT * FROM t_reg_fasor_")
                    .Append(day.ToString("yyyyMMdd"))
                    .Append(" WHERE ");

                if (day.Date.Equals(start.Date))
                {
                    sb.Append("tempo>=").Append(TimeUtils.Soc(start)).Append(" AND ");
                }

                if (day.Date.Equals(finish.Date))
                {
                    sb.Append("tempo<=").Append(TimeUtils.Soc(finish)).Append(" AND ");
                }

                sb.Append("idcodepmu=").Append(id).Append(" ");

                if (!string.IsNullOrWhiteSpace(channelsCsv))
                {
                    sb.Append("AND numchphasor IN (").Append(channelsCsv).Append(") ");
                }

                if (!string.IsNullOrWhiteSpace(rate))
                {
                    sb.Append(rate);
                }

                union = " UNION ALL ";
            }

            sb.Append(" ORDER BY tempo, cont");
            return sb.ToString();
        }

        // índice invertido: chave textual pedida -> Channel
        private static Dictionary<string, Channel> BuildInverseMeasurement(List<Channel> channels)
        {
            var result = new Dictionary<string, Channel>(StringComparer.OrdinalIgnoreCase);

            foreach (Channel channel in channels)
            {
                if (channel.Quantity != ChannelQuantity.VOLTAGE && channel.Quantity != ChannelQuantity.CURRENT)
                {
                    // FREQUENCY, DFREQ etc. entram pelo nome da grandeza
                    result[channel.Quantity.ToString()] = channel;
                }
                else
                {
                    // fasores: "id"+"0" = magnitude ; "id"+"1" = ângulo
                    result[channel.Id.ToString() + (int)channel.Value] = channel;
                }
            }

            return result;
        }

        public Dictionary<Channel, ITimeSeries> QueryTerminalSeries(
            string id,
            DateTime start,
            DateTime finish,
            List<Channel> measurements,
            int dataRate,
            int equipmentRate,
            bool downloadStat = false)
        {
            Console.WriteLine("QueryTerminal MedFasee");
            Console.Out.Flush();

            var builtMeasurements = BuildInverseMeasurement(measurements);
            Dictionary<Channel, ITimeSeries> series = null;

            try
            {
                using (var connection = new MySqlConnection(connectionString))
                {
                    string commandString = BuildQuery(
                        id, start, finish, GetChannels(measurements), GetRate(dataRate, equipmentRate));

                    using (var command = new MySqlCommand(commandString, connection))
                    {
                        command.CommandTimeout = COMMAND_TIMEOUT;
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            series = new Dictionary<Channel, ITimeSeries>();

                            // cria somente as séries requisitadas
                            foreach (var kv in builtMeasurements)
                            {
                                series[kv.Value] = new TimeSeries();
                            }

                            long startSoc = TimeUtils.Soc(start);
                            double startOle = TimeUtils.OaDate(start);

                            if (!reader.HasRows)
                            {
                                return series; // nada retornou → devolve vazio
                            }

                            // mapeia nomes de coluna -> índice (case-insensitive)
                            var ord = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                ord[reader.GetName(i)] = i;
                            }

                            // Tentar descobrir colunas por nome; fallback por posição (layout clássico)
                            int cMeas = ord.TryGetValue("measurement", out var tMeas) ? tMeas : (reader.FieldCount > 1 ? 1 : -1);
                            int cSec = ord.TryGetValue("sec", out var tSec) ? tSec : (reader.FieldCount > 2 ? 2 : -1);
                            int cMs = ord.TryGetValue("ms", out var tMs) ? tMs : (reader.FieldCount > 3 ? 3 : -1);

                            bool hasMag = ord.TryGetValue("magnitude", out var cMag) || ord.TryGetValue("mag", out cMag) || reader.FieldCount > 4;
                            if (!ord.ContainsKey("magnitude") && !ord.ContainsKey("mag"))
                            {
                                cMag = (reader.FieldCount > 4 ? 4 : -1);
                            }

                            bool hasAng = ord.TryGetValue("angle", out var cAng) || ord.TryGetValue("ang", out cAng) || reader.FieldCount > 5;
                            if (!ord.ContainsKey("angle") && !ord.ContainsKey("ang"))
                            {
                                cAng = (reader.FieldCount > 5 ? 5 : -1);
                            }

                            bool hasFreq = ord.TryGetValue("frequency", out var cFreq) || reader.FieldCount > 6;
                            if (!ord.ContainsKey("frequency"))
                            {
                                cFreq = (reader.FieldCount > 6 ? 6 : -1);
                            }

                            bool hasRocof = ord.TryGetValue("dfreq", out var cRocof) || reader.FieldCount > 7;
                            if (!ord.ContainsKey("dfreq"))
                            {
                                cRocof = (reader.FieldCount > 7 ? 7 : -1);
                            }

                            double lastTimeFreqEmit = double.NaN;

                            while (reader.Read())
                            {
                                // colunas mínimas para montar o timestamp
                                if (cMeas < 0 || cSec < 0 || cMs < 0 || reader.IsDBNull(cMeas) || reader.IsDBNull(cSec) || reader.IsDBNull(cMs))
                                {
                                    continue;
                                }

                                string measurementStr = reader.GetValue(cMeas).ToString();
                                long sec = Convert.ToInt64(reader.GetValue(cSec));
                                int ms = Convert.ToInt32(reader.GetValue(cMs));

                                double time = startOle + TimeUtils.SocDiff(startSoc, 0, sec, ms, equipmentRate);

                                // Magnitude (measurement + "0")
                                if (hasMag && cMag >= 0 &&
                                    builtMeasurements.TryGetValue(measurementStr + "0", out var chMag) &&
                                    !reader.IsDBNull(cMag))
                                {
                                    series[chMag].Add(time, Convert.ToDouble(reader.GetValue(cMag)));
                                }

                                // Ângulo (measurement + "1")
                                if (hasAng && cAng >= 0 &&
                                    builtMeasurements.TryGetValue(measurementStr + "1", out var chAng) &&
                                    !reader.IsDBNull(cAng))
                                {
                                    series[chAng].Add(time, Convert.ToDouble(reader.GetValue(cAng)));
                                }

                                // Frequência
                                if (hasFreq && cFreq >= 0 &&
                                    builtMeasurements.TryGetValue("FREQUENCY", out var chFreq) &&
                                    !reader.IsDBNull(cFreq))
                                {
                                    if (time != lastTimeFreqEmit)
                                    {
                                        series[chFreq].Add(time, Convert.ToDouble(reader.GetValue(cFreq)));
                                        lastTimeFreqEmit = time;
                                    }
                                }

                                // DFREQ
                                if (hasRocof && cRocof >= 0 &&
                                    builtMeasurements.TryGetValue("DFREQ", out var chRocof) &&
                                    !reader.IsDBNull(cRocof))
                                {
                                    // (não força unicidade de timestamp com freq; usa mesma janela)
                                    series[chRocof].Add(time, Convert.ToDouble(reader.GetValue(cRocof)));
                                }
                            }

                            // se FREQ/DFREQ foram pedidas mas não vieram colunas, tenta calcular
                            bool precisaFreq = builtMeasurements.ContainsKey("FREQUENCY") && !hasFreq;
                            bool precisaRocof = builtMeasurements.ContainsKey("DFREQ") && !hasRocof;

                            if (precisaFreq || precisaRocof)
                            {
                                foreach (var kv in CalculateFrequency(series, startOle, dataRate, equipmentRate))
                                {
                                    if (precisaFreq && kv.Key == "FREQUENCY")
                                    {
                                        series[builtMeasurements["FREQUENCY"]] = kv.Value;
                                    }

                                    if (precisaRocof && kv.Key == "DFREQ")
                                    {
                                        series[builtMeasurements["DFREQ"]] = kv.Value;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (MySqlException _ex)
            {
                if (_ex.Number == 0)
                {
                    throw new QueryTimeoutException();
                }
                else if (_ex.Number == 1042)
                {
                    throw new InvalidConnectionException(Ip, Database);
                }
                else if (_ex.Number == 1146)
                {
                    throw new InvalidQueryException(InvalidQueryException.NO_TABLE);
                }
                else
                {
                    throw new InvalidQueryException(_ex.Message);
                }
            }
            catch (Exception _ex)
            {
                throw new InvalidQueryException(_ex.Message);
            }

            return series;
        }

        private static List<KeyValuePair<string, ITimeSeries>> CalculateFrequency(
            Dictionary<Channel, ITimeSeries> measurements,
            double startOA,
            int framesPerSecond,
            int equipmentRate)
        {
            var result = new List<KeyValuePair<string, ITimeSeries>>();

            // só calcula se houver tensões A/B/C (mod e ang)
            if (!measurements.TryGetValue(Channel.VOLTAGE_A_MOD, out var vaMod) ||
                !measurements.TryGetValue(Channel.VOLTAGE_B_MOD, out var vbMod) ||
                !measurements.TryGetValue(Channel.VOLTAGE_C_MOD, out var vcMod) ||
                !measurements.TryGetValue(Channel.VOLTAGE_A_ANG, out var vaAng) ||
                !measurements.TryGetValue(Channel.VOLTAGE_B_ANG, out var vbAng) ||
                !measurements.TryGetValue(Channel.VOLTAGE_C_ANG, out var vcAng))
            {
                // sem base para cálculo → devolve listas vazias
                result.Add(new KeyValuePair<string, ITimeSeries>("FREQUENCY", new TimeSeries()));
                result.Add(new KeyValuePair<string, ITimeSeries>("DFREQ", new TimeSeries()));
                return result;
            }

            int phaseAIndex = 0, phaseBIndex = 0, phaseCIndex = 0;
            double lastFrequency = -999;
            double lastAngle = -999;

            var timestampsFreq = new List<double>();
            var calculatedFreq = new List<double>();
            var timestampsDfreq = new List<double>();
            var calculatedDfreq = new List<double>();

            while (phaseAIndex < vaMod.Count &&
                   phaseBIndex < vbMod.Count &&
                   phaseCIndex < vcMod.Count)
            {
                double phaseATime = vaMod.Timestamp(phaseAIndex);
                double phaseBTime = vbMod.Timestamp(phaseBIndex);
                double phaseCTime = vcMod.Timestamp(phaseCIndex);

                double maxTime = Math.Max(phaseATime, Math.Max(phaseBTime, phaseCTime));

                // Alinhamento de timestamps (gap preenchimento)
                while (phaseAIndex < vaMod.Count &&
                       phaseATime < maxTime &&
                       Math.Abs(phaseATime - maxTime) > 3 * TimeUtils.OA_MILLISECOND)
                {
                    lastAngle = -999;
                    lastFrequency = -999;
                    phaseAIndex++;
                    if (phaseAIndex < vaMod.Count) phaseATime = vaMod.Timestamp(phaseAIndex);
                }

                while (phaseBIndex < vbMod.Count &&
                       phaseBTime < maxTime &&
                       Math.Abs(phaseBTime - maxTime) > 3 * TimeUtils.OA_MILLISECOND)
                {
                    lastAngle = -999;
                    lastFrequency = -999;
                    phaseBIndex++;
                    if (phaseBIndex < vbMod.Count) phaseBTime = vbMod.Timestamp(phaseBIndex);
                }

                while (phaseCIndex < vcMod.Count &&
                       phaseCTime < maxTime &&
                       Math.Abs(phaseCTime - maxTime) > 3 * TimeUtils.OA_MILLISECOND)
                {
                    lastAngle = -999;
                    lastFrequency = -999;
                    phaseCIndex++;
                    if (phaseCIndex < vcMod.Count) phaseCTime = vcMod.Timestamp(phaseCIndex);
                }

                if (phaseAIndex >= vaMod.Count || phaseBIndex >= vbMod.Count || phaseCIndex >= vcMod.Count)
                {
                    break;
                }

                double phaseAVoltage = vaMod.Reading(phaseAIndex);
                double phaseBVoltage = vbMod.Reading(phaseBIndex);
                double phaseCVoltage = vcMod.Reading(phaseCIndex);

                double phaseAAngle = vaAng.Reading(phaseAIndex) * Math.PI / 180;
                double phaseBAngle = (vbAng.Reading(phaseBIndex) + 120) * Math.PI / 180;
                double phaseCAngle = (vcAng.Reading(phaseCIndex) + 240) * Math.PI / 180;

                // Cálculo da sequência positiva (Sincronização de PLL)
                double positiveSequenceAngle = Math.Atan2(
                    phaseAVoltage * Math.Sin(phaseAAngle) +
                    phaseBVoltage * Math.Sin(phaseBAngle) +
                    phaseCVoltage * Math.Sin(phaseCAngle),
                    phaseAVoltage * Math.Cos(phaseAAngle) +
                    phaseBVoltage * Math.Cos(phaseBAngle) +
                    phaseCVoltage * Math.Cos(phaseCAngle)
                ) * 180 / Math.PI;

                if (lastAngle != -999)
                {
                    double diff = positiveSequenceAngle - lastAngle;
                    if (diff > 180)
                    {
                        diff -= 360;
                    }
                    else if (diff < -180)
                    {
                        diff += 360;
                    }

                    // Cálculo da frequência
                    double freq = diff / (360.0 / framesPerSecond) + equipmentRate;

                    double ts = maxTime;
                    timestampsFreq.Add(ts);
                    calculatedFreq.Add(freq);

                    if (lastFrequency != -999)
                    {
                        // Cálculo do RoCoF (Rate of Change of Frequency)
                        timestampsDfreq.Add(ts);
                        calculatedDfreq.Add((freq - lastFrequency) * framesPerSecond);
                    }
                    lastFrequency = freq;
                }

                lastAngle = positiveSequenceAngle;
                phaseAIndex++;
                phaseBIndex++;
                phaseCIndex++;
            }

            // Inserção de pontos iniciais para evitar gaps/erros de derivada no início
            if (timestampsFreq.Count != 0)
            {
                timestampsFreq.Insert(0, timestampsFreq[0] - 1000.0 * TimeUtils.OA_MILLISECOND / framesPerSecond);
                calculatedFreq.Insert(0, calculatedFreq[0]);
            }
            if (timestampsDfreq.Count != 0)
            {
                timestampsDfreq.Insert(0, timestampsDfreq[0] - 1000.0 * TimeUtils.OA_MILLISECOND / framesPerSecond);
                calculatedDfreq.Insert(0, calculatedDfreq[0]);
                // Inserir outro ponto para garantir o cálculo do primeiro RoCoF (se necessário)
                timestampsDfreq.Insert(0, timestampsDfreq[0] - 1000.0 * TimeUtils.OA_MILLISECOND / framesPerSecond);
                calculatedDfreq.Insert(0, calculatedDfreq[0]);
            }

            result.Add(new KeyValuePair<string, ITimeSeries>("FREQUENCY", new TimeSeries(timestampsFreq, calculatedFreq)));
            result.Add(new KeyValuePair<string, ITimeSeries>("DFREQ", new TimeSeries(timestampsDfreq, calculatedDfreq)));
            return result;
        }
    }
}
