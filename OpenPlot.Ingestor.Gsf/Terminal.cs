using System;
using System.Collections.Generic;

namespace OpenPlot.Ingestor.Gsf
{
    public class Terminal
    {
        public string Id { get; private set; }
        public int IdNumber { get; private set; }
        public string DisplayName { get; private set; }

        public int EquipmentRate { get; private set; }
        public double VoltageLevel { get; private set; }

        public string Area { get; set; }
        public string State { get; set; }
        public string Station { get; set; }

        public List<Channel> Channels { get; private set; }

        public Terminal(string id, int idNumber, string displayName, int equipmentRate, double voltageLevel, string area, string state, string station) :
            this(id, idNumber, displayName, equipmentRate, voltageLevel, area, state, station, new List<Channel>())
        {
        }

        public Terminal(string id, int idNumber, string displayName, int equipmentRate, double voltageLevel, string area, string state, string station, List<Channel> channels)
        {
            Id = id;
            IdNumber = idNumber;
            DisplayName = displayName;
            EquipmentRate = equipmentRate;
            VoltageLevel = voltageLevel;
            Area = area;
            State = state;
            Station = station;
            Channels = channels;
        }


    }
}
