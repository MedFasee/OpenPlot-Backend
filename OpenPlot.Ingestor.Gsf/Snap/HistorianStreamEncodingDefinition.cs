using System;
using SnapDB.Snap;
using SnapDB.Snap.Definitions;
using SnapDB.Snap.Encoding;

namespace OpenPlot.Ingestor.Gsf.Snap;

/// <summary>
/// Definition for the HistorianStreamEncoding pair encoding.
/// GUID must match openHistorian's HistorianStreamEncodingDefinition: 0418B3A7-F631-47AF-BBFA-8B9BC0378328
/// </summary>
internal class HistorianStreamEncodingDefinition : PairEncodingDefinitionBase
{
    // {0418B3A7-F631-47AF-BBFA-8B9BC0378328}
    public static readonly EncodingDefinition TypeGuid = new EncodingDefinition(
        new Guid(0x0418b3a7, 0xf631, 0x47af, 0xbb, 0xfa, 0x8b, 0x9b, 0xc0, 0x37, 0x83, 0x28));

    public override Type KeyTypeIfNotGeneric => typeof(HistorianKey);

    public override Type ValueTypeIfNotGeneric => typeof(HistorianValue);

    public override EncodingDefinition Method => TypeGuid;

    public override PairEncodingBase<TKey, TValue> Create<TKey, TValue>()
    {
        return (PairEncodingBase<TKey, TValue>)(object)new HistorianStreamEncoding();
    }
}
