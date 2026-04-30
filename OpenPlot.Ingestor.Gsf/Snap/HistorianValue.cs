using System;
using Gemstone;
using SnapDB.IO;
using SnapDB.Snap;

namespace OpenPlot.Ingestor.Gsf.Snap
{
    public class HistorianValue : SnapTypeBase<HistorianValue>
    {
        public ulong Value1;
        public ulong Value2;
        public ulong Value3;

        public override Guid GenericTypeGuid => new("24DDE7DC-67F9-42B6-A11B-E27C3E62D9EF");

        public float AsSingle
        {
            get => BitConvert.ToSingle(Value1);
            set => Value1 = BitConvert.ToUInt64(value);
        }

        public override int Size => 24;

        public override void Clear()
        {
            Value1 = 0;
            Value2 = 0;
            Value3 = 0;
        }

        public override void SetMin()
        {
            Value1 = 0;
            Value2 = 0;
            Value3 = 0;
        }

        public override void SetMax()
        {
            Value1 = ulong.MaxValue;
            Value2 = ulong.MaxValue;
            Value3 = ulong.MaxValue;
        }

        public override void CopyTo(HistorianValue destination)
        {
            destination.Value1 = Value1;
            destination.Value2 = Value2;
            destination.Value3 = Value3;
        }

        public override int CompareTo(HistorianValue? other)
        {
            if (other is null)
                return 1;

            int result = Value1.CompareTo(other.Value1);

            if (result != 0)
                return result;

            result = Value2.CompareTo(other.Value2);

            if (result != 0)
                return result;

            return Value3.CompareTo(other.Value3);
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
    }
}