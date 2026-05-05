namespace Gtlm.Hdfs.Client.Protocol;

using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Parsed checksum metadata from the DataNode's OP_READ_BLOCK response.
/// </summary>
internal readonly record struct ChecksumInfo(
    ChecksumTypeProto ChecksumType,
    int BytesPerChecksum,
    long ChunkOffset);
