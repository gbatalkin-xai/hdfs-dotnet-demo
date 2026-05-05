namespace Gtlm.Hdfs.Client.Models;

/// <summary>
/// Identifies a unique block across the HDFS cluster.
/// Wraps the poolId + blockId + generationStamp triple.
/// </summary>
public readonly record struct ExtendedBlock(
    string PoolId,
    long BlockId,
    long GenerationStamp,
    long NumBytes = 0)
{
    internal Proto.ExtendedBlockProto ToProto() => new()
    {
        PoolId = PoolId,
        BlockId = (ulong)BlockId,
        GenerationStamp = (ulong)GenerationStamp,
        NumBytes = (ulong)NumBytes,
    };

    internal static ExtendedBlock FromProto(Proto.ExtendedBlockProto proto) => new(
        PoolId: proto.PoolId,
        BlockId: (long)proto.BlockId,
        GenerationStamp: (long)proto.GenerationStamp,
        NumBytes: (long)proto.NumBytes);

    public override string ToString() =>
        $"Block(pool={PoolId}, id={BlockId}, gs={GenerationStamp}, bytes={NumBytes})";
}
