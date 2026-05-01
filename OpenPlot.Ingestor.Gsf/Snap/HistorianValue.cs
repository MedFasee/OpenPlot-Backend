using System;
using Gemstone;
using SnapDB;
using SnapDB.IO;
using SnapDB.Snap;

namespace OpenPlot.Ingestor.Gsf.Snap;

/// <summary>
/// The standard value used in the OpenHistorian.
/// </summary>
public class HistorianValue : SnapTypeBase<HistorianValue>
{
    // {24DDE7DC-67F9-42B6-A11B-E27C3E62D9EF}
    private static readonly Guid s_typeGuid = new(0x24dde7dc, 0x67f9, 0x42b6, 0xa1, 0x1b, 0xe2, 0x7c, 0x3e, 0x62, 0xd9, 0xef);
    private const int StructureSize = 24;

    /// <summary>Value1 — first 64 bits; use this field for 32-bit values.</summary>
    public ulong Value1;

    /// <summary>Value2 — only when value cannot fit in Value1.</summary>
    public ulong Value2;

    /// <summary>Value3 — digital/quality data.</summary>
    public ulong Value3;

    public float AsSingle
    {
        get => BitConvert.ToSingle(Value1);
        set => Value1 = BitConvert.ToUInt64(value);
    }

    public override Guid GenericTypeGuid => s_typeGuid;

    public override int Size => StructureSize;

    public override void CopyTo(HistorianValue destination)
    {
        destination.Value1 = Value1;
        destination.Value2 = Value2;
        destination.Value3 = Value3;
    }

    public override int CompareTo(HistorianValue other)
    {
        if (Value1 != other.Value1) return Value1 < other.Value1 ? -1 : 1;
        if (Value2 != other.Value2) return Value2 < other.Value2 ? -1 : 1;
        if (Value3 != other.Value3) return Value3 < other.Value3 ? -1 : 1;
        return 0;
    }

    public override unsafe int CompareTo(byte* stream)
    {
        ulong v1 = *(ulong*)stream;
        ulong v2 = *(ulong*)(stream + 8);
        ulong v3 = *(ulong*)(stream + 16);
        if (Value1 != v1) return Value1 < v1 ? -1 : 1;
        if (Value2 != v2) return Value2 < v2 ? -1 : 1;
        if (Value3 != v3) return Value3 < v3 ? -1 : 1;
        return 0;
    }

    public override bool IsLessThan(HistorianValue right)
    {
        if (Value1 != right.Value1) return Value1 < right.Value1;
        if (Value2 != right.Value2) return Value2 < right.Value2;
        return Value3 < right.Value3;
    }

    public override bool IsEqualTo(HistorianValue right) =>
        Value1 == right.Value1 && Value2 == right.Value2 && Value3 == right.Value3;

    public override bool IsGreaterThan(HistorianValue right)
    {
        if (Value1 != right.Value1) return Value1 > right.Value1;
        if (Value2 != right.Value2) return Value2 > right.Value2;
        return Value3 > right.Value3;
    }

    public override bool IsGreaterThanOrEqualTo(HistorianValue right)
    {
        if (Value1 != right.Value1) return Value1 > right.Value1;
        if (Value2 != right.Value2) return Value2 > right.Value2;
        return Value3 >= right.Value3;
    }

    public override unsafe void Read(byte* stream)
    {
        Value1 = *(ulong*)stream;
        Value2 = *(ulong*)(stream + 8);
        Value3 = *(ulong*)(stream + 16);
    }

    public override unsafe void Write(byte* stream)
    {
        *(ulong*)stream = Value1;
        *(ulong*)(stream + 8) = Value2;
        *(ulong*)(stream + 16) = Value3;
    }

    public override void Read(BinaryStreamBase stream)
    {
        Value1 = stream.ReadUInt64();
        Value2 = stream.ReadUInt64();
        Value3 = stream.ReadUInt64();
    }

    public override void Write(BinaryStreamBase stream)
    {
        stream.Write(Value1);
        stream.Write(Value2);
        stream.Write(Value3);
    }

    public override void SetMin()
    {
        Value1 = ulong.MinValue;
        Value2 = ulong.MinValue;
        Value3 = ulong.MinValue;
    }

    public override void SetMax()
    {
        Value1 = ulong.MaxValue;
        Value2 = ulong.MaxValue;
        Value3 = ulong.MaxValue;
    }

    public override void Clear()
    {
        Value1 = 0;
        Value2 = 0;
        Value3 = 0;
    }
}
