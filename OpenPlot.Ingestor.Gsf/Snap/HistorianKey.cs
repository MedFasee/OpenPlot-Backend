using System;
using Gemstone;
using SnapDB.IO;
using SnapDB.Snap;
using SnapDB.Snap.Types;

namespace OpenPlot.Ingestor.Gsf.Snap;

/// <summary>
/// The standard key used in the OpenHistorian.
/// </summary>
public class HistorianKey : TimestampPointIDBase<HistorianKey>
{
    // {6527D41B-9D04-4BFA-8133-05273D521D46}
    private static readonly Guid s_typeGuid = new(0x6527d41b, 0x9d04, 0x4bfa, 0x81, 0x33, 0x05, 0x27, 0x3d, 0x52, 0x1d, 0x46);
    private const int StructureSize = 24;

    public ulong EntryNumber;

    public override Guid GenericTypeGuid => s_typeGuid;

    public override int Size => StructureSize;

    public DateTime TimestampAsDate
    {
        get => new DateTime((long)Timestamp);
        set => Timestamp = (ulong)value.Ticks;
    }

    public override void CopyTo(HistorianKey destination)
    {
        destination.Timestamp = Timestamp;
        destination.PointID = PointID;
        destination.EntryNumber = EntryNumber;
    }

    public override int CompareTo(HistorianKey other)
    {
        if (Timestamp != other.Timestamp)
            return Timestamp < other.Timestamp ? -1 : 1;
        if (PointID != other.PointID)
            return PointID < other.PointID ? -1 : 1;
        if (EntryNumber != other.EntryNumber)
            return EntryNumber < other.EntryNumber ? -1 : 1;
        return 0;
    }

    public override unsafe int CompareTo(byte* stream)
    {
        ulong ts = *(ulong*)stream;
        ulong pid = *(ulong*)(stream + 8);
        ulong en = *(ulong*)(stream + 16);
        if (Timestamp != ts) return Timestamp < ts ? -1 : 1;
        if (PointID != pid) return PointID < pid ? -1 : 1;
        if (EntryNumber != en) return EntryNumber < en ? -1 : 1;
        return 0;
    }

    public override bool IsLessThan(HistorianKey right)
    {
        if (Timestamp != right.Timestamp) return Timestamp < right.Timestamp;
        if (PointID != right.PointID) return PointID < right.PointID;
        return EntryNumber < right.EntryNumber;
    }

    public override bool IsEqualTo(HistorianKey right) =>
        Timestamp == right.Timestamp && PointID == right.PointID && EntryNumber == right.EntryNumber;

    public override bool IsGreaterThan(HistorianKey right)
    {
        if (Timestamp != right.Timestamp) return Timestamp > right.Timestamp;
        if (PointID != right.PointID) return PointID > right.PointID;
        return EntryNumber > right.EntryNumber;
    }

    public override bool IsGreaterThanOrEqualTo(HistorianKey right)
    {
        if (Timestamp != right.Timestamp) return Timestamp > right.Timestamp;
        if (PointID != right.PointID) return PointID > right.PointID;
        return EntryNumber >= right.EntryNumber;
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

    public override void SetMin()
    {
        Timestamp = ulong.MinValue;
        PointID = ulong.MinValue;
        EntryNumber = ulong.MinValue;
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

    public override string ToString() =>
        Timestamp <= (ulong)DateTime.MaxValue.Ticks
            ? TimestampAsDate.ToString("yyyy-MM-dd HH:mm:ss.fffffff") + "/" + PointID
            : Timestamp + "/" + PointID;
}
