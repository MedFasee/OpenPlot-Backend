using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Xml.Linq;
using System.Xml.Schema;


namespace Medi.Ingestor.Gsf
{
    public class SystemData
    {
        public int NominalFrequency { get; }
        public string Ip { get; }
        public int Port { get; }
        public string Name { get; }
        public DatabaseType Type { get; }
        public string User { get; }
        public string Password { get; }
        public string Database { get; }
        public List<Terminal> Terminals { get; private set; }

        public SystemData(string ip, int port, string name, int nominalFrequency, DatabaseType type, string user, string pass, string db)
        {
            Ip = ip;
            Port = port;
            Name = name;
            NominalFrequency = nominalFrequency;
            Type = type;
            User = user;
            Password = pass;
            Database = db;
            Terminals = new List<Terminal>();
        }

        private static SystemData BuildPdcConfig(XNamespace nameSpace, XElement system)
        {
            SystemData result = null;
            try
            {
                var name = (string)system.Element(nameSpace + "name");
                var fps = (int)system.Element(nameSpace + "fps");
                var address = (string)system.Element(nameSpace + "address");
                var securityUser = (string)system.Element(nameSpace + "security").Element(nameSpace + "user");
                var securityPswd = (string)system.Element(nameSpace + "security").Element(nameSpace + "pswd");
                var db = (string)system.Element(nameSpace + "dataBank");
                int port;

                var typeString = (string)system.Element(nameSpace + "type");
                DatabaseType type;

                switch (typeString)
                {
                    case "medfasee":
                        type = DatabaseType.Medfasee;
                        break;
                    case "openpdc":
                        type = DatabaseType.Historian_OpenPDC;
                        break;
                    case "openhistorian2":
                        type = DatabaseType.Historian2_OpenHistorian2;
                        break;
                    default:
                        throw new ArgumentException("Invalid database type specified.");
                }

                string[] addressParts = address.Split(':');

                if (address.ToLower().Contains("https"))
                {

                    address = addressParts[0] + ":" + addressParts[1];

                    if (addressParts.Length == 2)
                    {
                        port = type == DatabaseType.Historian_OpenPDC ? 6152 : 3306;
                    }
                    else
                    {
                        port = int.Parse(addressParts[addressParts.Length - 1]);
                    }
                }

                else
                {
                    address = addressParts[0];
                    if (addressParts.Length == 1)
                    {
                        port = type == DatabaseType.Historian_OpenPDC ? 6152 : 3306;
                    }
                    else
                    {
                        port = int.Parse(addressParts[addressParts.Length - 1]);
                    }
                }


                result = new SystemData(address, port, name, fps, type, securityUser, securityPswd, db);
            }
            catch (ArgumentNullException e)
            {
                throw new FormatException("Invalid format for PDC Node, there was no " + e.Source + " child node", e);
            }
            catch (FormatException e)
            {
                throw new FormatException(string.Format("Invalid {0} supplied!", e.Source), e);
            }

            return result;
        }

        private static Terminal ParseTerminal(XNamespace nameSpace, XElement terminal, int nominalFrequency, DatabaseType dbType)
        {
            Terminal result;

            try
            {
                var voltageLevel = (double)terminal.Element(nameSpace + "voltLevel") / 1000;
                var area = (string)terminal.Element(nameSpace + "local").Element(nameSpace + "area");
                var state = (string)terminal.Element(nameSpace + "local").Element(nameSpace + "state");
                var station = (string)terminal.Element(nameSpace + "local").Element(nameSpace + "station");
                var idName = (string)terminal.Element(nameSpace + "idName");
                var idNumber = terminal.Element(nameSpace + "idNumber") == null ? -1 : (int)terminal.Element(nameSpace + "idNumber");
                var fullName = (string)terminal.Element(nameSpace + "fullName");
                var equipmentRate = terminal.Element(nameSpace + "equipmentRate") == null ? nominalFrequency : (int)terminal.Element(nameSpace + "equipmentRate");
                var channels = terminal.Element(nameSpace + "measurements").Elements();

                result = new Terminal(idName, idNumber, idName, equipmentRate, voltageLevel, area, state, station, ParseChannels(nameSpace, channels, dbType));
            }
            catch (ArgumentNullException e)
            {
                throw new FormatException("Invalid format for PMU Node, there was no " + e.Source + " child node");
            }

            return result;
        }

        private static List<Channel> ParseChannels(XNamespace nameSpace, IEnumerable<XElement> channels, DatabaseType dbType)
        {
            List<Channel> result = new List<Channel>();
            try
            {
                foreach (var channel in channels)
                {
                    string channelName = channel.Name.LocalName.ToLower();
                    switch (channelName)
                    {

                        case ("phasor"):
                            var name = (string)channel.Element(nameSpace + "pName");
                            var type = (string)channel.Element(nameSpace + "pType");
                            var phase = (string)channel.Element(nameSpace + "pPhase");

                            int modId = -1, angId = -1;

                            if (dbType == DatabaseType.Historian_OpenPDC || dbType == DatabaseType.Historian2_OpenHistorian2)
                            {
                                modId = (int)channel.Element(nameSpace + "modId");
                                angId = (int)channel.Element(nameSpace + "angId");
                            }
                            else if (dbType == DatabaseType.Medfasee)
                            {
                                modId = angId = (int)channel.Element(nameSpace + "chId");
                            }

                            result.Add(new Channel(modId, name, Channel.GetPhaseFromString(phase), ChannelValueType.ABSOLUTE, Channel.GetQuantityFromString(type)));
                            result.Add(new Channel(angId, name, Channel.GetPhaseFromString(phase), ChannelValueType.ANGLE, Channel.GetQuantityFromString(type)));
                            break;
                        case ("analog"):
                            name = (string)channel.Element(nameSpace + "aName");
                            type = (string)channel.Element(nameSpace + "aType");
                            phase = (string)channel.Element(nameSpace + "aPhase");
                            var aId = (int)channel.Element(nameSpace + "aId");
                            if (type == "CTHD")
                                result.Add(new Channel(aId, name, Channel.GetPhaseFromString(phase), ChannelValueType.CTHD, ChannelQuantity.ANALOG));
                            if (type == "VTHD")
                                result.Add(new Channel(aId, name, Channel.GetPhaseFromString(phase), ChannelValueType.VTHD, ChannelQuantity.ANALOG));
                            if (type == "VIMB")
                                result.Add(new Channel(aId, name, ChannelPhase.NONE, ChannelValueType.VIMB, ChannelQuantity.ANALOG));
                            if (type == "CIMB")
                                result.Add(new Channel(aId, name, ChannelPhase.NONE, ChannelValueType.CIMB, ChannelQuantity.ANALOG));
                            break;
                        case ("digital"):
                            name = (string)channel.Element(nameSpace + "dName");
                            type = (string)channel.Element(nameSpace + "dType");
                            phase = (string)channel.Element(nameSpace + "dPhase");
                            var dId = (int)channel.Element(nameSpace + "dId");
                            if (type == "CFDS")
                                result.Add(new Channel(dId, name, ChannelPhase.NONE, ChannelValueType.CFDS, ChannelQuantity.DIGITAL));
                            break;
                        case ("freq"):
                            name = (string)channel.Element(nameSpace + "fName");
                            var fId = (int)channel.Element(nameSpace + "fId");
                            result.Add(new Channel(fId, name, ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.FREQUENCY));
                            break;
                        case ("dfreq"):
                            name = (string)channel.Element(nameSpace + "dfName");
                            var dfId = (int)channel.Element(nameSpace + "dfId");
                            result.Add(new Channel(dfId, name, ChannelPhase.NONE, ChannelValueType.NONE, ChannelQuantity.DFREQ));
                            break;

                        default:
                            name = (string)channel.Element(nameSpace + "name");
                            phase = (string)channel.Element(nameSpace + "phase");
                            var id = (int)channel.Element(nameSpace + "id");
                            result.Add(new Channel(id, name, Channel.GetPhaseFromString(phase), ChannelValueType.NONE, Channel.GetQuantityFromString(channelName)));
                            break;
                    }
                }
            }
            catch (ArgumentNullException e)
            {
                throw new FormatException("Invalid format for Measurement Node, there was no " + e.Source + " child node");
            }


            return result;
        }

        public static SystemData ReadConfig(string cfgFile)
        {

            SystemData result = null;
            XElement root = XElement.Load(cfgFile);
            XNamespace ns = "smsf2";

            #region PDC info

            var pdc = root.Element(ns + "pdc");

            if (pdc == null)
                throw new FormatException("No PDC Node found on configuration file");

            result = BuildPdcConfig(ns, pdc);

            #endregion

            // Extração das PMus a partir do arquivo XML

            List<Terminal> terminals = new List<Terminal>();



            foreach (var terminal in root.Elements(ns + "pmu"))
            {
                terminals.Add(ParseTerminal(ns, terminal, result.NominalFrequency, result.Type));
            }

            // Ordenação para apresentação na árvore
            result.Terminals = terminals.OrderBy(p => p.Area).ThenBy(p => p.State).ThenBy(p => p.Station).ThenBy(p => p.VoltageLevel).ThenBy(p => p.Id).ToList();

            return result;
        }
    }
}
