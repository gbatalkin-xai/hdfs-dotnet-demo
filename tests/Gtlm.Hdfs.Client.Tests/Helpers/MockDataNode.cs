namespace Gtlm.Hdfs.Client.Tests.Helpers;

using System.IO.Pipelines;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Simulates a DataNode for testing RemoteBlockReader.
/// Builds a byte stream containing the OP_READ_BLOCK response + data packets.
/// </summary>
internal static class MockDataNode
{
    public static DatanodeInfo DefaultDn => new()
    {
        IpAddress = "127.0.0.1",
        HostName = "mock-dn",
        DatanodeUuid = "mock-uuid",
        XferPort = 9866,
    };

    /// <summary>
    /// Build a complete mock DataNode response stream:
    /// 1. BlockOpResponseProto (SUCCESS + checksum info)
    /// 2. Data packets with CRC32C checksums
    /// 3. Empty last packet
    ///
    /// Returns (readStream, writeStream, dataNode) for creating a Peer.
    /// The readStream contains the response; writeStream captures client writes.
    /// </summary>
    public static (Peer peer, byte[] expectedData) CreateMockPeer(
        byte[] blockData,
        int bytesPerChecksum = 512,
        int packetSize = 64 * 1024,
        long startOffset = 0,
        long? readLength = null,
        ChecksumTypeProto checksumType = ChecksumTypeProto.ChecksumCrc32C)
    {
        long length = readLength ?? blockData.Length - startOffset;
        long chunkOffset = startOffset - (startOffset % bytesPerChecksum);

        // Build response bytes
        using var responseStream = new MemoryStream();

        // 1. BlockOpResponseProto
        var response = new BlockOpResponseProto
        {
            Status = Status.Success,
            ReadOpChecksumInfo = new ReadOpChecksumInfoProto
            {
                Checksum = new ChecksumProto
                {
                    Type = checksumType,
                    BytesPerChecksum = (uint)bytesPerChecksum,
                },
                ChunkOffset = (ulong)chunkOffset,
            },
        };
        response.WriteDelimitedTo(responseStream);

        // 2. Data packets
        // Include alignment prefix data from chunkOffset to startOffset
        long dataStart = chunkOffset;
        long dataEnd = startOffset + length;
        int totalWireBytes = (int)(dataEnd - dataStart);

        Func<ReadOnlySpan<byte>, uint> checksumFunc = checksumType switch
        {
            ChecksumTypeProto.ChecksumCrc32C => Crc32CChecksum.ComputeCrc32C,
            ChecksumTypeProto.ChecksumCrc32 => Crc32Checksum.ComputeIeeeCrc32,
            _ => _ => 0,
        };

        int offset = (int)dataStart;
        long seqno = 0;
        while (offset < (int)dataEnd)
        {
            int remaining = (int)dataEnd - offset;
            int pktDataLen = Math.Min(packetSize, remaining);
            var pktData = blockData[offset..(offset + pktDataLen)];
            bool isLast = offset + pktDataLen >= (int)dataEnd;

            var pktBytes = PacketBuilder.Build(
                offsetInBlock: offset,
                seqno: seqno++,
                lastPacketInBlock: false,
                data: pktData,
                bytesPerChecksum: bytesPerChecksum,
                checksumFunc: checksumFunc);

            responseStream.Write(pktBytes);
            offset += pktDataLen;
        }

        // 3. Empty last packet
        var lastPkt = PacketBuilder.BuildEmptyLast(offset, seqno);
        responseStream.Write(lastPkt);

        // Create streams
        var readStream = new MemoryStream(responseStream.ToArray());
        var writeStream = new MemoryStream();

        var peer = Peer.CreateForTest(readStream, writeStream, DefaultDn);

        // Expected data: the portion the reader should return
        var expectedData = blockData[(int)startOffset..(int)(startOffset + length)];

        return (peer, expectedData);
    }
}
