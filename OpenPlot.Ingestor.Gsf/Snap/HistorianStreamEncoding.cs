using SnapDB.IO;
using SnapDB.Snap;
using SnapDB.Snap.Definitions;
using SnapDB.Snap.Encoding;

namespace OpenPlot.Ingestor.Gsf.Snap;

/// <summary>
/// Stream encoding compatible with openHistorian's default historian stream encoding.
/// GUID: 0418B3A7-F631-47AF-BBFA-8B9BC0378328
/// </summary>
internal class HistorianStreamEncoding : PairEncodingBase<HistorianKey, HistorianValue>
{
    public override EncodingDefinition EncodingMethod => HistorianStreamEncodingDefinition.TypeGuid;

    public override bool UsesPreviousKey => true;

    public override bool UsesPreviousValue => false;

    public override int MaxCompressionSize => 55;

    public override bool ContainsEndOfStreamSymbol => true;

    public override byte EndOfStreamSymbol => 255;

    public override void Encode(BinaryStreamBase stream, HistorianKey prevKey, HistorianValue prevValue, HistorianKey key, HistorianValue value)
    {
        if (key.Timestamp == prevKey.Timestamp
            && key.EntryNumber == 0
            && value.Value1 <= uint.MaxValue
            && value.Value2 == 0
            && value.Value3 == 0)
        {
            ulong pointDelta = key.PointID ^ prevKey.PointID;
            if (pointDelta <= 63)
            {
                if (value.Value1 == 0)
                {
                    stream.Write((byte)pointDelta);
                    return;
                }

                stream.Write((byte)(64u | pointDelta));
                stream.Write((uint)value.Value1);
                return;
            }
        }

        byte code = 128;

        if (key.Timestamp != prevKey.Timestamp)
            code |= 64;

        if (key.EntryNumber != 0)
            code |= 32;

        if (value.Value1 > uint.MaxValue)
            code |= 16;
        else if (value.Value1 > 0)
            code |= 8;

        if (value.Value2 != 0)
            code |= 4;

        if (value.Value3 > uint.MaxValue)
            code |= 2;
        else if (value.Value3 > 0)
            code |= 1;

        stream.Write(code);

        if (key.Timestamp != prevKey.Timestamp)
            stream.Write7Bit(key.Timestamp ^ prevKey.Timestamp);

        stream.Write7Bit(key.PointID ^ prevKey.PointID);

        if (key.EntryNumber != 0)
            stream.Write7Bit(key.EntryNumber);

        if (value.Value1 > uint.MaxValue)
            stream.Write(value.Value1);
        else if (value.Value1 > 0)
            stream.Write((uint)value.Value1);

        if (value.Value2 != 0)
            stream.Write(value.Value2);

        if (value.Value3 > uint.MaxValue)
            stream.Write(value.Value3);
        else if (value.Value3 > 0)
            stream.Write((uint)value.Value3);
    }

    public override void Decode(BinaryStreamBase stream, HistorianKey prevKey, HistorianValue prevValue, HistorianKey key, HistorianValue value, out bool isEndOfStream)
    {
        isEndOfStream = false;
        byte code = stream.ReadUInt8();
        if (code == 255)
        {
            isEndOfStream = true;
            return;
        }

        if (code < 128)
        {
            if (code < 64)
            {
                key.Timestamp = prevKey.Timestamp;
                key.PointID = prevKey.PointID ^ code;
                key.EntryNumber = 0;
                value.Value1 = 0;
                value.Value2 = 0;
                value.Value3 = 0;
            }
            else
            {
                key.Timestamp = prevKey.Timestamp;
                key.PointID = prevKey.PointID ^ code ^ 64ul;
                key.EntryNumber = 0;
                value.Value1 = stream.ReadUInt32();
                value.Value2 = 0;
                value.Value3 = 0;
            }

            return;
        }

        if ((code & 64) != 0)
            key.Timestamp = prevKey.Timestamp ^ stream.Read7BitUInt64();
        else
            key.Timestamp = prevKey.Timestamp;

        key.PointID = prevKey.PointID ^ stream.Read7BitUInt64();

        if ((code & 32) != 0)
            key.EntryNumber = stream.Read7BitUInt64();
        else
            key.EntryNumber = 0;

        if ((code & 16) != 0)
            value.Value1 = stream.ReadUInt64();
        else if ((code & 8) != 0)
            value.Value1 = stream.ReadUInt32();
        else
            value.Value1 = 0;

        if ((code & 4) != 0)
            value.Value2 = stream.ReadUInt64();
        else
            value.Value2 = 0;

        if ((code & 2) != 0)
            value.Value3 = stream.ReadUInt64();
        else if ((code & 1) != 0)
            value.Value3 = stream.ReadUInt32();
        else
            value.Value3 = 0;
    }

    public override PairEncodingBase<HistorianKey, HistorianValue> Clone() => new HistorianStreamEncoding();
}
