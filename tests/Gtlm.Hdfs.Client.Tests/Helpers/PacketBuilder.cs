namespace Gtlm.Hdfs.Client.Tests.Helpers;

using System.Buffers.Binary;
using Google.Protobuf;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Proto;

/// <summary>
/// Builds synthetic HDFS data packets for testing.
/// </summary>
internal static class PacketBuilder
{
    /// <summary>
    /// Build a complete binary packet matching the HDFS wire format.
    /// </summary>
    public static byte[] Build(
        long offsetInBlock,
        long seqno,
        bool lastPacketInBlock,
        byte[] data,
        int bytesPerChecksum,
        Func<ReadOnlySpan<byte>, uint> checksumFunc)
    {
        var header = new PacketHeaderProto
        {
            OffsetInBlock = offsetInBlock,
            Seqno = seqno,
            LastPacketInBlock = lastPacketInBlock,
            DataLen = data.Length,
        };
        byte[] headerBytes = header.ToByteArray();

        int numChunks = data.Length == 0 ? 0
            : (data.Length + bytesPerChecksum - 1) / bytesPerChecksum;
        byte[] checksums = new byte[numChunks * 4];
        for (int i = 0; i < numChunks; i++)
        {
            int start = i * bytesPerChecksum;
            int end = Math.Min(start + bytesPerChecksum, data.Length);
            uint crc = checksumFunc(data.AsSpan(start, end - start));
            BinaryPrimitives.WriteUInt32BigEndian(checksums.AsSpan(i * 4), crc);
        }

        // packetLen = 2 (headerLen field) + headerBytes.Length + checksums.Length + data.Length
        int packetLen = 2 + headerBytes.Length + checksums.Length + data.Length;
        byte[] packet = new byte[4 + packetLen];

        int pos = 0;
        BinaryPrimitives.WriteInt32BigEndian(packet.AsSpan(pos), packetLen);
        pos += 4;
        BinaryPrimitives.WriteInt16BigEndian(packet.AsSpan(pos), (short)headerBytes.Length);
        pos += 2;
        headerBytes.CopyTo(packet.AsSpan(pos));
        pos += headerBytes.Length;
        checksums.CopyTo(packet.AsSpan(pos));
        pos += checksums.Length;
        data.CopyTo(packet.AsSpan(pos));

        return packet;
    }

    /// <summary>
    /// Build a packet with CRC32C checksums.
    /// </summary>
    public static byte[] BuildCrc32C(
        long offsetInBlock, long seqno, bool last, byte[] data, int bpc = 512)
    {
        return Build(offsetInBlock, seqno, last, data, bpc, Crc32CChecksum.ComputeCrc32C);
    }

    /// <summary>
    /// Build an empty last packet (signals end of block).
    /// </summary>
    public static byte[] BuildEmptyLast(long offsetInBlock, long seqno)
    {
        return Build(offsetInBlock, seqno, lastPacketInBlock: true,
            data: [], bytesPerChecksum: 512, _ => 0);
    }
}
