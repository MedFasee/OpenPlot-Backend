using OpenPlot.Ingestor.Gsf;
using OpenPlot.Ingestor.Gsf.Data;
using OpenPlot.Ingestor.Gsf.Repository;
using OpenPlot.Ingestor.Gsf.Utils;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class MeasurementMedFasee : Database, IMeasurementDb
    {

        const int COMMAND_TIMEOUT = 100;

        string Database { get; }
        readonly string connectionString;
        public MeasurementMedFasee(string ip, int port, string user, string pass, string database) : base(ip, user, pass)
        {
            Database = database;

            connectionString = "server =" + ip + ";" +
                        "Port =" + port + ";" +
                        "User ID =" + user + ";" +
                        "Password =" + pass + ";" +
                        "database =" + database + ";" +
                        "connection timeout= 100";
        }

        private static string GetRate(int dataRate, int equipmentRate)
        {
            if (equipmentRate / dataRate == equipmentRate)
                return " and (cont=0)";
            if (equipmentRate / dataRate != 1)
                return " and (cont%" + (equipmentRate / dataRate).ToString("0.#") + "=0)";
            else
                return "";


        }

        private static string GetChannels(List<Channel> measurements)
        {
            string prefix = "";
            StringBuilder sb = new StringBuilder();
            foreach (Channel channel in measurements)
            {
                if (channel.Value != ChannelValueType.ABSOLUTE)
                    continue;
                sb.Append(prefix);
                sb.Append(channel.Id);
                prefix = ",";
            }
            return sb.ToString();
        }

        private static string BuildQuery(string id, DateTime start, DateTime finish, string channels, string rate)
        {
            string prefix = "";

            StringBuilder sb = new StringBuilder();

            foreach (DateTime day in Utils.TimeUtils.EachDay(start, finish))
            {
                sb.Append(prefix);

                sb.Append("SELECT DISTINCT * FROM t_reg_fasor_");
                sb.Append(day.ToString("yyyyMMdd")).Append(" where ");

                if (day.Date.Equals(start.Date))
                    sb.Append("tempo >=" + Utils.TimeUtils.Soc(start) + " and ");

                if (day.Date.Equals(finish.Date))
                    sb.Append("tempo <=" + Utils.TimeUtils.Soc(finish) + " and ");

                sb.Append("idcodepmu =").Append(id).Append(" and ");
                sb.Append("numchphasor in (").Append(channels).Append(")");
                sb.Append(rate);
                prefix = " UNION ALL ";
            }
            sb.Append(" order by tempo, cont");



            return sb.ToString();

        }

        private static Dictionary<string, Channel> BuildInverseMeasurement(List<Channel> channels)
        {
            Dictionary<string, Channel> result = new Dictionary<string, Channel>();

            foreach (Channel channel in channels)
            {
                if (channel.Quantity != ChannelQuantity.VOLTAGE && channel.Quantity != ChannelQuantity.CURRENT)
                    result[channel.Quantity.ToString()] = channel;
                else
                    result[channel.Id.ToString() + (int)channel.Value] = channel;
            }

            return result;
        }

        public Dictionary<Channel, ITimeSeries> QueryTerminalSeries(string id, DateTime start, DateTime finish, List<Channel> measurements, int dataRate, int equipmentRate, bool downloadStat = false)
        {

            Console.WriteLine("QueryTarminal MedFasee");
            Console.Out.Flush();

            Dictionary<string, Channel> builtMeasurements = BuildInverseMeasurement(measurements);

            Dictionary<Channel, ITimeSeries> series = null;

            try
            {

                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    string commandString = BuildQuery(id, start, finish, GetChannels(measurements), GetRate(dataRate, equipmentRate));

                    using (MySqlCommand command = new MySqlCommand(commandString, connection))
                    {
                        command.CommandTimeout = COMMAND_TIMEOUT;
                        connection.Open();
                        MySqlDataReader reader = command.ExecuteReader();

                        series = new Dictionary<Channel, ITimeSeries>();

                        foreach (KeyValuePair<string, Channel> pair in builtMeasurements)
                            series.Add(pair.Value, new TimeSeries());

                        long startSoc = Utils.TimeUtils.Soc(start);
                        double startOle = Utils.TimeUtils.OaDate(start);

                        bool frequencyReading = false;
                        bool rocofReading = false;

                        if (reader.FieldCount > 6)
                        {
                            frequencyReading = true;

                        }

                        if (reader.FieldCount > 7)
                        {
                            rocofReading = true;

                        }

                        double lastFreqOrRocof = 0;

                        if (!reader.HasRows)
                            throw new InvalidQueryException(InvalidQueryException.EMPTY);

                        while (reader.Read())
                        {
                            string measurement = reader[1].ToString();

                            double time = startOle + Utils.TimeUtils.SocDiff(startSoc, 0, Convert.ToInt64(reader[2]), Convert.ToInt32(reader[3]), equipmentRate);


                            series[builtMeasurements[measurement + "0"]].Add(time, Convert.ToDouble(reader[4]));
                            series[builtMeasurements[measurement + "1"]].Add(time, Convert.ToDouble(reader[5]));

                            if (lastFreqOrRocof != time)
                            {
                                if (frequencyReading)
                                    series[builtMeasurements["FREQUENCY"]].Add(time, Convert.ToDouble(reader[6]));
                                if (rocofReading)
                                    series[builtMeasurements["DFREQ"]].Add(time, Convert.ToDouble(reader[7]));

                                lastFreqOrRocof = time;
                            }

                        }

                        if ((builtMeasurements.ContainsKey("FREQUENCY") && !frequencyReading) || (builtMeasurements.ContainsKey("DFREQ") && !rocofReading))
                        {
                            foreach (var calculatedData in CalculateFrequency(series, startOle, dataRate, equipmentRate))
                            {
                                if (builtMeasurements.ContainsKey("FREQUENCY") && !frequencyReading && calculatedData.Key == "FREQUENCY")
                                    series[builtMeasurements["FREQUENCY"]] = calculatedData.Value;
                                if (builtMeasurements.ContainsKey("DFREQ") && !rocofReading && calculatedData.Key == "DFREQ")
                                    series[builtMeasurements["DFREQ"]] = calculatedData.Value;
                            }

                        }
                    }
                }
            }
            catch (MySqlException _ex)
            {
                if (_ex.Number == 0)
                    throw new QueryTimeoutException();
                else if (_ex.Number == 1042)
                    throw new InvalidConnectionException(Ip, Database);
                else if (_ex.Number == 1146)
                    throw new InvalidQueryException(InvalidQueryException.NO_TABLE);
                else
                    throw new InvalidQueryException(_ex.Message);
            }
            catch (Exception _ex)
            {
                throw new InvalidQueryException(_ex.Message);
            }


            return series;
        }

        private static List<KeyValuePair<string, ITimeSeries>> CalculateFrequency(Dictionary<Channel, ITimeSeries> measurements, double startOA, int framesPerSecond, int equipmentRate)
        {
            List<KeyValuePair<string, ITimeSeries>> result = new List<KeyValuePair<string, ITimeSeries>>();

            int phaseAIndex = 0;
            int phaseBIndex = 0;
            int phaseCIndex = 0;

            double lastFrequency = -999;
            double lastAngle = -999;

            List<double> timestampsFreq = new List<double>();
            List<double> calculatedFreq = new List<double>();
            List<double> timestampsDfreq = new List<double>();
            List<double> calculatedDfreq = new List<double>();


            while (phaseAIndex < measurements[Channel.VOLTAGE_A_MOD].Count &&
                phaseBIndex < measurements[Channel.VOLTAGE_B_MOD].Count &&
                phaseCIndex < measurements[Channel.VOLTAGE_C_MOD].Count)
            {

                double phaseATime = measurements[Channel.VOLTAGE_A_MOD].Timestamp(phaseAIndex);
                double phaseBTime = measurements[Channel.VOLTAGE_B_MOD].Timestamp(phaseBIndex);
                double phaseCTime = measurements[Channel.VOLTAGE_C_MOD].Timestamp(phaseCIndex);

                double maxTime = Math.Max(phaseATime, Math.Max(phaseBTime, phaseCTime));

                while (phaseATime < maxTime &&
                    Math.Abs(phaseATime - maxTime) > 3 * TimeUtils.OA_MILLISECOND &&
                    phaseAIndex < measurements[Channel.VOLTAGE_A_MOD].Count)
                {
                    lastAngle = -999;
                    lastFrequency = -999;
                    phaseAIndex++;
                    phaseATime = measurements[Channel.VOLTAGE_A_MOD].Timestamp(phaseAIndex);
                }

                while (phaseBTime < maxTime &&
                    Math.Abs(phaseBTime - maxTime) > 3 * TimeUtils.OA_MILLISECOND &&
                    phaseBIndex < measurements[Channel.VOLTAGE_B_MOD].Count)
                {
                    lastAngle = -999;
                    lastFrequency = -999;
                    phaseBIndex++;
                    phaseBTime = measurements[Channel.VOLTAGE_B_MOD].Timestamp(phaseBIndex);
                }

                while (phaseCTime < maxTime &&
                    Math.Abs(phaseCTime - maxTime) > 3 * TimeUtils.OA_MILLISECOND &&
                    phaseCIndex < measurements[Channel.VOLTAGE_C_MOD].Count)
                {
                    lastAngle = -999;
                    lastFrequency = -999;
                    phaseCIndex++;
                    phaseCTime = measurements[Channel.VOLTAGE_C_MOD].Timestamp(phaseCIndex);
                }

                double phaseAVoltage = measurements[Channel.VOLTAGE_A_MOD].Reading(phaseAIndex);
                double phaseBVoltage = measurements[Channel.VOLTAGE_B_MOD].Reading(phaseBIndex);
                double phaseCVoltage = measurements[Channel.VOLTAGE_C_MOD].Reading(phaseCIndex);

                double phaseAAngle = measurements[Channel.VOLTAGE_A_ANG].Reading(phaseAIndex) * Math.PI / 180;
                double phaseBAngle = (measurements[Channel.VOLTAGE_B_ANG].Reading(phaseBIndex) + 120) * Math.PI / 180;
                double phaseCAngle = (measurements[Channel.VOLTAGE_C_ANG].Reading(phaseCIndex) + 240) * Math.PI / 180;


                double positiveSequenceAngle = Math.Atan2(phaseAVoltage * Math.Sin(phaseAAngle) +
                    phaseBVoltage * Math.Sin(phaseBAngle) +
                    phaseCVoltage * Math.Sin(phaseCAngle),
                    phaseAVoltage * Math.Cos(phaseAAngle) +
                    phaseBVoltage * Math.Cos(phaseBAngle) +
                    phaseCVoltage * Math.Cos(phaseCAngle)) * 180 / Math.PI;

                if (lastAngle != -999)
                {
                    double diff = positiveSequenceAngle - lastAngle;

                    if (diff > 180)
                        diff = diff - 360;
                    else if (diff < -180)
                        diff = diff + 360;

                    double freq = diff / (360 / framesPerSecond) + equipmentRate;

                    timestampsFreq.Add(maxTime);
                    calculatedFreq.Add(freq);

                    if (lastFrequency != -999)
                    {
                        timestampsDfreq.Add(maxTime);
                        calculatedDfreq.Add((freq - lastFrequency) * framesPerSecond);
                    }
                    lastFrequency = freq;
                }
                lastAngle = positiveSequenceAngle;
                phaseAIndex++;
                phaseBIndex++;
                phaseCIndex++;
            }
            if (timestampsFreq.Count != 0)
            {
                timestampsFreq.Insert(0, timestampsFreq[0] - 1000.0 * TimeUtils.OA_MILLISECOND / framesPerSecond);
                calculatedFreq.Insert(0, calculatedFreq[0]);
            }
            if (timestampsDfreq.Count != 0)
            {
                timestampsDfreq.Insert(0, timestampsDfreq[0] - 1000.0 * TimeUtils.OA_MILLISECOND / framesPerSecond);
                calculatedDfreq.Insert(0, calculatedDfreq[0]);
                timestampsDfreq.Insert(0, timestampsDfreq[0] - 1000.0 * TimeUtils.OA_MILLISECOND / framesPerSecond);
                calculatedDfreq.Insert(0, calculatedDfreq[0]);
            }
            result.Add(new KeyValuePair<string, ITimeSeries>("FREQUENCY", new TimeSeries(timestampsFreq, calculatedFreq)));
            result.Add(new KeyValuePair<string, ITimeSeries>("DFREQ", new TimeSeries(timestampsDfreq, calculatedDfreq)));

            return result;

        }
    }
}
