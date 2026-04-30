using System;
using Gemstone;
using SnapDB.IO;
using SnapDB.Snap.Types;

namespace OpenPlot.Ingestor.Gsf.Snap
{
    public class HistorianKey : TimestampPointIDBase<HistorianKey>
    {
        public ulong EntryNumber;

        public override Guid GenericTypeGuid => new("6527D41B-9D04-4BFA-8133-05273D521D46");

        public DateTime TimestampAsDate
        {
            get => new((long)Timestamp);
            set => Timestamp = (ulong)value.Ticks;
        }

        public long MillisecondTimestamp
        {
            get => (long)(Timestamp / TimeSpan.TicksPerMillisecond);
            set => Timestamp = (ulong)(value * TimeSpan.TicksPerMillisecond);
        }

        public override int Size => 24;

        public override void SetMin()
        {
            Timestamp = 0;
            PointID = 0;
            EntryNumber = 0;
        }

        public override void SetMax()
        {
            Timestamp = ulong.MaxValue;
            PointID = ulong.MaxValue;
            EntryNumber = ulong.MaxValue;
        }

        public override void Clear()
        {
            Timestamp = 0;
            PointID = 0;
            EntryNumber = 0;
        }

        public override void CopyTo(HistorianKey destination)
        {
            destination.Timestamp = Timestamp;
            destination.PointID = PointID;
            destination.EntryNumber = EntryNumber;
        }

        public override int CompareTo(HistorianKey other)
        {
            int result = Timestamp.CompareTo(other.Timestamp);

            if (result != 0)
                return result;

            result = PointID.CompareTo(other.PointID);

            if (result != 0)
                return result;

            return EntryNumber.CompareTo(other.EntryNumber);
        }

        public override bool IsLessThan(HistorianKey key)
        {
            if (Timestamp != key.Timestamp)
                return Timestamp < key.Timestamp;

            if (PointID != key.PointID)
                return PointID < key.PointID;

            return EntryNumber < key.EntryNumber;
        }

        public override bool IsEqualTo(HistorianKey key)
        {
            return Timestamp == key.Timestamp && PointID == key.PointID && EntryNumber == key.EntryNumber;
        }

        public override bool IsGreaterThan(HistorianKey key)
        {
            if (Timestamp != key.Timestamp)
                return Timestamp > key.Timestamp;

            if (PointID != key.PointID)
                return PointID > key.PointID;

            return EntryNumber > key.EntryNumber;
        }

        public override bool IsGreaterThanOrEqualTo(HistorianKey key)
        {
            return !IsLessThan(key);
        }

        public override void Read(BinaryStreamBase stream)
        {
            Timestamp = stream.ReadUInt64();
            PointID = stream.ReadUInt64();
            EntryNumber = stream.ReadUInt64();
        }

        public override void Write(BinaryStreamBase stream)
        {
            stream.Write(Timestamp);
            stream.Write(PointID);
            stream.Write(EntryNumber);
        }

        public override unsafe void Read(byte* stream)
        {
            Timestamp = *(ulong*)stream;
            PointID = *(ulong*)(stream + 8);
            EntryNumber = *(ulong*)(stream + 16);
        }

        public override unsafe void Write(byte* stream)
        {
            *(ulong*)stream = Timestamp;
            *(ulong*)(stream + 8) = PointID;
            *(ulong*)(stream + 16) = EntryNumber;
        }
    }
}