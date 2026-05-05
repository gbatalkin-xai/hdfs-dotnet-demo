namespace Gtlm.Hdfs.Client.Tests;

using System.IO.Pipelines;
using Gtlm.Hdfs.Client.Checksum;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Proto;
using Gtlm.Hdfs.Client.Tests.Helpers;

public class PacketReaderTests
{
    private static DataChecksum MakeChecksum(int bpc = 512) =>
        DataChecksum.Create(ChecksumTypeProto.ChecksumCrc32C, bpc);

    /// <summary>
    /// Feed packet bytes into a Pipe and read via PacketReader.
    /// </summary>
    private static async Task<PacketData> ReadSinglePacketAsync(byte[] packetBytes, int bpc = 512)
    {
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: long.MaxValue));
        await pipe.Writer.WriteAsync(packetBytes);
        await pipe.Writer.CompleteAsync();

        var reader = new PacketReader(pipe.Reader, MakeChecksum(bpc));
        return await reader.ReadNextPacketAsync();
    }

    [Fact]
    public async Task SinglePacket_ParsesCorrectly()
    {
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var packetBytes = PacketBuilder.BuildCrc32C(
            offsetInBlock: 0, seqno: 0, last: false, data: data, bpc: 8);

        var packet = await ReadSinglePacketAsync(packetBytes, bpc: 8);

        Assert.Equal(0L, packet.OffsetInBlock);
        Assert.Equal(0L, packet.SeqNo);
        Assert.False(packet.IsLastPacket);
        Assert.Equal(8, packet.DataLength);
        Assert.Equal(data, packet.Data.ToArray());
    }

    [Fact]
    public async Task EmptyLastPacket_ParsedCorrectly()
    {
        var packetBytes = PacketBuilder.BuildEmptyLast(offsetInBlock: 1024, seqno: 5);

        var packet = await ReadSinglePacketAsync(packetBytes);

        Assert.Equal(1024L, packet.OffsetInBlock);
        Assert.Equal(5L, packet.SeqNo);
        Assert.True(packet.IsLastPacket);
        Assert.Equal(0, packet.DataLength);
        Assert.Empty(packet.Data.ToArray());
        Assert.Empty(packet.Checksums.ToArray());
    }

    [Fact]
    public async Task MultiplePackets_Sequential()
    {
        var data1 = new byte[] { 10, 20, 30, 40 };
        var data2 = new byte[] { 50, 60, 70, 80 };

        var pkt1 = PacketBuilder.BuildCrc32C(0, 0, false, data1, bpc: 4);
        var pkt2 = PacketBuilder.BuildCrc32C(4, 1, false, data2, bpc: 4);
        var pktLast = PacketBuilder.BuildEmptyLast(8, 2);

        // Concatenate all packets into one stream
        var combined = new byte[pkt1.Length + pkt2.Length + pktLast.Length];
        pkt1.CopyTo(combined, 0);
        pkt2.CopyTo(combined, pkt1.Length);
        pktLast.CopyTo(combined, pkt1.Length + pkt2.Length);

        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: long.MaxValue));
        await pipe.Writer.WriteAsync(combined);
        await pipe.Writer.CompleteAsync();

        var reader = new PacketReader(pipe.Reader, MakeChecksum(bpc: 4));

        var p1 = await reader.ReadNextPacketAsync();
        Assert.Equal(data1, p1.Data.ToArray());
        Assert.Equal(0L, p1.SeqNo);
        Assert.False(p1.IsLastPacket);

        var p2 = await reader.ReadNextPacketAsync();
        Assert.Equal(data2, p2.Data.ToArray());
        Assert.Equal(1L, p2.SeqNo);
        Assert.False(p2.IsLastPacket);

        var p3 = await reader.ReadNextPacketAsync();
        Assert.True(p3.IsLastPacket);
        Assert.Equal(0, p3.DataLength);
    }

    [Fact]
    public async Task LargePacket_64KB()
    {
        var data = new byte[64 * 1024];
        Random.Shared.NextBytes(data);

        var packetBytes = PacketBuilder.BuildCrc32C(0, 0, false, data, bpc: 512);

        var packet = await ReadSinglePacketAsync(packetBytes, bpc: 512);

        Assert.Equal(64 * 1024, packet.DataLength);
        Assert.Equal(data, packet.Data.ToArray());

        // Should have 128 checksums (64K / 512 = 128, * 4 bytes = 512 bytes)
        Assert.Equal(512, packet.Checksums.Length);
    }

    [Fact]
    public async Task Checksums_AreCorrect()
    {
        int bpc = 4;
        var data = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
        var packetBytes = PacketBuilder.BuildCrc32C(0, 0, false, data, bpc);

        var packet = await ReadSinglePacketAsync(packetBytes, bpc);

        // Verify the checksums from the packet match what we'd compute
        var checksum = MakeChecksum(bpc);
        checksum.VerifyChunks(packet.Data.Span, packet.Checksums.Span, offsetInBlock: 0);
    }

    [Fact]
    public async Task PartialChunk_ChecksumsCorrect()
    {
        int bpc = 4;
        // 6 bytes = 2 chunks: [4] + [2]
        var data = new byte[] { 1, 2, 3, 4, 5, 6 };
        var packetBytes = PacketBuilder.BuildCrc32C(0, 0, false, data, bpc);

        var packet = await ReadSinglePacketAsync(packetBytes, bpc);

        Assert.Equal(6, packet.DataLength);
        Assert.Equal(8, packet.Checksums.Length); // 2 chunks * 4 bytes

        var checksum = MakeChecksum(bpc);
        checksum.VerifyChunks(packet.Data.Span, packet.Checksums.Span, offsetInBlock: 0);
    }

    [Fact]
    public async Task HeartbeatPacket_HasSeqnoMinusOne()
    {
        // Build a heartbeat-like packet: seqno=-1, dataLen=0
        var packetBytes = PacketBuilder.Build(
            offsetInBlock: 0, seqno: -1, lastPacketInBlock: false,
            data: [], bytesPerChecksum: 512, _ => 0);

        var packet = await ReadSinglePacketAsync(packetBytes);

        Assert.Equal(-1L, packet.SeqNo);
        Assert.Equal(0, packet.DataLength);
        Assert.False(packet.IsLastPacket);
    }

    [Fact]
    public async Task ConnectionClosed_ThrowsEndOfStream()
    {
        var pipe = new Pipe();
        // Write only 2 bytes, then complete (not enough for a 4-byte packetLen)
        await pipe.Writer.WriteAsync(new byte[] { 0x00, 0x01 });
        await pipe.Writer.CompleteAsync();

        var reader = new PacketReader(pipe.Reader, MakeChecksum());

        await Assert.ThrowsAsync<EndOfStreamException>(
            () => reader.ReadNextPacketAsync().AsTask());
    }

    [Fact]
    public async Task ConnectionClosed_MidPacket_ThrowsEndOfStream()
    {
        var pipe = new Pipe();
        // Write packetLen + headerLen but no header content
        var buf = new byte[6];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buf, 100); // packetLen=100
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(4), 20); // headerLen=20
        await pipe.Writer.WriteAsync(buf);
        await pipe.Writer.CompleteAsync(); // EOF before header bytes

        var reader = new PacketReader(pipe.Reader, MakeChecksum());

        await Assert.ThrowsAsync<EndOfStreamException>(
            () => reader.ReadNextPacketAsync().AsTask());
    }

    [Fact]
    public async Task IncrementalWrite_WaitsForMoreData()
    {
        var pipe = new Pipe();
        var data = new byte[] { 0xFF, 0xFE };
        var packetBytes = PacketBuilder.BuildCrc32C(0, 0, true, data, bpc: 2);

        // Write packet in small drips
        var readTask = Task.Run(async () =>
        {
            var reader = new PacketReader(pipe.Reader, MakeChecksum(bpc: 2));
            return await reader.ReadNextPacketAsync();
        });

        // Feed bytes one at a time
        for (int i = 0; i < packetBytes.Length; i++)
        {
            await pipe.Writer.WriteAsync(new byte[] { packetBytes[i] });
            await pipe.Writer.FlushAsync();
            await Task.Delay(1);
        }
        await pipe.Writer.CompleteAsync();

        var packet = await readTask;
        Assert.Equal(data, packet.Data.ToArray());
        Assert.True(packet.IsLastPacket);
    }

    [Fact]
    public async Task OffsetInBlock_PreservedCorrectly()
    {
        var data = new byte[16];
        var packetBytes = PacketBuilder.BuildCrc32C(
            offsetInBlock: 134217728, seqno: 42, last: false, data: data, bpc: 16);

        var packet = await ReadSinglePacketAsync(packetBytes, bpc: 16);

        Assert.Equal(134217728L, packet.OffsetInBlock); // 128 MB
        Assert.Equal(42L, packet.SeqNo);
    }
}
