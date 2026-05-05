namespace Gtlm.Hdfs.Client.Protocol;

using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Parsed packet from the DataNode's packet stream.
/// Holds the checksum and data regions as owned byte arrays.
/// </summary>
internal readonly struct PacketData
{
    /// <summary>Parsed protobuf header.</summary>
    public PacketHeaderProto Header { get; init; }

    /// <summary>Checksum bytes (one CRC per chunk of data).</summary>
    public ReadOnlyMemory<byte> Checksums { get; init; }

    /// <summary>Block data payload.</summary>
    public ReadOnlyMemory<byte> Data { get; init; }

    /// <summary>True if this is the final packet in the block.</summary>
    public bool IsLastPacket => Header.LastPacketInBlock;

    /// <summary>Byte offset of this packet's data within the block.</summary>
    public long OffsetInBlock => Header.OffsetInBlock;

    /// <summary>Length of the data payload.</summary>
    public int DataLength => Header.DataLen;

    /// <summary>Sequence number (-1 for heartbeats).</summary>
    public long SeqNo => Header.Seqno;
}
