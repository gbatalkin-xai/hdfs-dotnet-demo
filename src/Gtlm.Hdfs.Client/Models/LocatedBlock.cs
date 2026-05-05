namespace Gtlm.Hdfs.Client.Models;

/// <summary>
/// A block together with its DataNode locations and access token.
/// Returned by the NameNode for getBlockLocations.
/// </summary>
public sealed class LocatedBlock
{
    public required ExtendedBlock Block { get; init; }

    /// <summary>Byte offset of this block within the file.</summary>
    public required long Offset { get; init; }

    /// <summary>DataNode locations sorted by network distance (nearest first).</summary>
    public required IReadOnlyList<DatanodeInfo> Locations { get; init; }

    /// <summary>Security token for accessing this block.</summary>
    public required BlockToken Token { get; init; }

    public bool IsLastBlock { get; init; }

    internal static LocatedBlock FromProto(Proto.LocatedBlockProto proto) => new()
    {
        Block = ExtendedBlock.FromProto(proto.B),
        Offset = (long)proto.Offset,
        Locations = proto.Locs
            .Select(DatanodeInfo.FromProto)
            .ToList(),
        Token = BlockToken.FromProto(proto.BlockToken),
    };
}
