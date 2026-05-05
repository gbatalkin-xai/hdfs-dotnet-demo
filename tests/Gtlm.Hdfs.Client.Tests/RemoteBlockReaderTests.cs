namespace Gtlm.Hdfs.Client.Tests;

using Google.Protobuf;
using Gtlm.Hdfs.Client.BlockReading;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Protocol;
using Gtlm.Hdfs.Client.Tests.Helpers;

public class RemoteBlockReaderTests
{
    private static readonly ExtendedBlock TestBlock = new("BP-1", 42, 100, 128 * 1024);
    private static readonly HdfsClientOptions TestOptions = new();

    private static async Task<RemoteBlockReader> CreateReader(
        Peer peer, long startOffset = 0, long? length = null, long blockSize = 0)
    {
        long len = length ?? blockSize;
        return await RemoteBlockReader.CreateAsync(
            file: "/test/file.dat",
            block: TestBlock,
            token: BlockToken.Empty,
            startOffset: startOffset,
            length: len,
            verifyChecksum: true,
            clientName: "test-client",
            peer: peer,
            peerCache: null,
            options: TestOptions);
    }

    [Fact]
    public async Task SimpleRead_SmallBlock_ReturnsCorrectData()
    {
        var blockData = new byte[1024];
        Random.Shared.NextBytes(blockData);

        var (peer, expected) = MockDataNode.CreateMockPeer(blockData);
        await using var reader = await CreateReader(peer, length: blockData.Length);

        var result = new byte[blockData.Length];
        int totalRead = 0;
        while (totalRead < result.Length)
        {
            int read = await reader.ReadAsync(result.AsMemory(totalRead));
            if (read == 0) break;
            totalRead += read;
        }

        Assert.Equal(blockData.Length, totalRead);
        Assert.Equal(expected, result);
    }

    [Fact]
    public async Task SimpleRead_ExactlyOneChunk_512Bytes()
    {
        var blockData = new byte[512];
        Random.Shared.NextBytes(blockData);

        var (peer, expected) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 512);
        await using var reader = await CreateReader(peer, length: 512);

        var result = new byte[512];
        await ReadFullyAsync(reader, result);
        Assert.Equal(expected, result);
    }

    [Fact]
    public async Task CopyToAsync_ReadsEntireBlock()
    {
        var blockData = new byte[8192];
        Random.Shared.NextBytes(blockData);

        var (peer, expected) = MockDataNode.CreateMockPeer(blockData, packetSize: 2048);
        await using var reader = await CreateReader(peer, length: blockData.Length);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task SmallBufferReads_StillReturnsAllData()
    {
        var blockData = new byte[2048];
        Random.Shared.NextBytes(blockData);

        var (peer, expected) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 512);
        await using var reader = await CreateReader(peer, length: blockData.Length);

        // Read in tiny 7-byte chunks (not aligned to anything)
        using var ms = new MemoryStream();
        var buf = new byte[7];
        int read;
        while ((read = await reader.ReadAsync(buf)) > 0)
        {
            ms.Write(buf, 0, read);
        }

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task ReadAsync_ReturnsZero_AfterAllBytesRead()
    {
        var blockData = new byte[100];
        Random.Shared.NextBytes(blockData);

        var (peer, _) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 100);
        await using var reader = await CreateReader(peer, length: 100);

        var buf = new byte[200];
        var read1 = await reader.ReadAsync(buf);
        Assert.Equal(100, read1);

        var read2 = await reader.ReadAsync(buf);
        Assert.Equal(0, read2);
    }

    [Fact]
    public async Task Position_TracksCorrectly()
    {
        var blockData = new byte[256];
        Random.Shared.NextBytes(blockData);

        var (peer, _) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 256);
        await using var reader = await CreateReader(peer, length: 256);

        Assert.Equal(0, reader.Position);

        var buf = new byte[100];
        await reader.ReadAsync(buf);
        Assert.Equal(100, reader.Position);

        await reader.ReadAsync(buf);
        Assert.Equal(200, reader.Position);

        buf = new byte[56];
        await reader.ReadAsync(buf);
        Assert.Equal(256, reader.Position);
        Assert.True(reader.IsComplete);
    }

    [Fact]
    public async Task Length_ReturnsRequestedLength()
    {
        var blockData = new byte[512];
        var (peer, _) = MockDataNode.CreateMockPeer(blockData);
        await using var reader = await CreateReader(peer, length: 512);

        Assert.Equal(512, reader.Length);
    }

    [Fact]
    public async Task StreamProperties_Correct()
    {
        var blockData = new byte[512];
        var (peer, _) = MockDataNode.CreateMockPeer(blockData);
        await using var reader = await CreateReader(peer, length: 512);

        Assert.True(reader.CanRead);
        Assert.False(reader.CanSeek);
        Assert.False(reader.CanWrite);
        Assert.Throws<NotSupportedException>(() => reader.Position = 10);
        Assert.Throws<NotSupportedException>(() => reader.Seek(0, SeekOrigin.Begin));
        Assert.Throws<NotSupportedException>(() => reader.SetLength(100));
        Assert.Throws<NotSupportedException>(() => reader.Write([], 0, 0));
    }

    [Fact]
    public async Task MultiPacket_LargeBlock_Correct()
    {
        var blockData = new byte[32 * 1024]; // 32 KB
        Random.Shared.NextBytes(blockData);

        // Use small packets (4KB) to force multiple packets
        var (peer, expected) = MockDataNode.CreateMockPeer(
            blockData, bytesPerChecksum: 512, packetSize: 4096);
        await using var reader = await CreateReader(peer, length: blockData.Length);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task NonAlignedOffset_SkipsCorrectly()
    {
        // Block has 2048 bytes. Read starting at offset 300 for 500 bytes.
        var blockData = new byte[2048];
        Random.Shared.NextBytes(blockData);

        var (peer, expected) = MockDataNode.CreateMockPeer(
            blockData, bytesPerChecksum: 512, startOffset: 300, readLength: 500);
        await using var reader = await CreateReader(peer, startOffset: 300, length: 500);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(500, ms.Length);
        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task Dispose_ReturnsPeerToCache_OnCleanClose()
    {
        var blockData = new byte[64];
        Random.Shared.NextBytes(blockData);

        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var (peer, _) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 64);

        var reader = await RemoteBlockReader.CreateAsync(
            "/test/file", TestBlock, BlockToken.Empty,
            0, 64, true, "client", peer, cache, TestOptions);

        // Read all data
        var buf = new byte[64];
        await ReadFullyAsync(reader, buf);

        // Dispose should return peer to cache
        await reader.DisposeAsync();

        // Peer should be retrievable from cache
        var cached = cache.TryGet(MockDataNode.DefaultDn);
        Assert.NotNull(cached);

        await cache.DisposeAsync();
    }

    [Fact]
    public async Task Dispose_DisposesPeer_OnNoCache()
    {
        var blockData = new byte[64];
        Random.Shared.NextBytes(blockData);

        var (peer, _) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 64);

        var reader = await RemoteBlockReader.CreateAsync(
            "/test/file", TestBlock, BlockToken.Empty,
            0, 64, true, "client", peer, peerCache: null, TestOptions);

        var buf = new byte[64];
        await ReadFullyAsync(reader, buf);

        await reader.DisposeAsync();
        Assert.True(peer.IsClosed);
    }

    [Fact]
    public async Task CreateAsync_ErrorStatus_ThrowsHdfsProtocolException()
    {
        // Build a response with ERROR status (no data packets)
        using var responseStream = new MemoryStream();
        var errorResponse = new Proto.BlockOpResponseProto
        {
            Status = Proto.Status.Error,
            Message = "block not found on this DataNode",
        };
        errorResponse.WriteDelimitedTo(responseStream);
        responseStream.Position = 0;

        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, MockDataNode.DefaultDn);

        var ex = await Assert.ThrowsAsync<HdfsProtocolException>(async () =>
            await CreateReader(peer, length: 1024));

        Assert.Contains("block not found", ex.Message);
    }

    [Fact]
    public async Task CreateAsync_AccessTokenError_ThrowsAccessTokenException()
    {
        using var responseStream = new MemoryStream();
        var errorResponse = new Proto.BlockOpResponseProto
        {
            Status = Proto.Status.ErrorAccessToken,
            Message = "token expired",
        };
        errorResponse.WriteDelimitedTo(responseStream);
        responseStream.Position = 0;

        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, MockDataNode.DefaultDn);

        await Assert.ThrowsAsync<AccessTokenException>(async () =>
            await CreateReader(peer, length: 1024));
    }

    [Fact]
    public async Task ReadAsync_CorruptChecksum_ThrowsChecksumException()
    {
        // Build a valid response + a packet with corrupted checksums
        using var responseStream = new MemoryStream();

        var response = new Proto.BlockOpResponseProto
        {
            Status = Proto.Status.Success,
            ReadOpChecksumInfo = new Proto.ReadOpChecksumInfoProto
            {
                Checksum = new Proto.ChecksumProto
                {
                    Type = Proto.ChecksumTypeProto.ChecksumCrc32C,
                    BytesPerChecksum = 512,
                },
                ChunkOffset = 0,
            },
        };
        response.WriteDelimitedTo(responseStream);

        // Build a packet with wrong checksums
        var data = new byte[512];
        Random.Shared.NextBytes(data);
        var corruptPacket = PacketBuilder.Build(
            offsetInBlock: 0, seqno: 0, lastPacketInBlock: false,
            data: data, bytesPerChecksum: 512,
            checksumFunc: _ => 0xDEADBEEF); // Wrong checksum
        responseStream.Write(corruptPacket);

        responseStream.Position = 0;
        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, MockDataNode.DefaultDn);

        await using var reader = await CreateReader(peer, length: 512);

        var buf = new byte[512];
        await Assert.ThrowsAsync<ChecksumException>(async () =>
            await reader.ReadAsync(buf));
    }

    [Fact]
    public async Task Dispose_SendsChecksumOk_OnCleanRead()
    {
        var blockData = new byte[64];
        Random.Shared.NextBytes(blockData);

        // Build mock response manually so we can hold the write stream reference
        using var responseStream = new MemoryStream();
        var response = new Proto.BlockOpResponseProto
        {
            Status = Proto.Status.Success,
            ReadOpChecksumInfo = new Proto.ReadOpChecksumInfoProto
            {
                Checksum = new Proto.ChecksumProto
                {
                    Type = Proto.ChecksumTypeProto.ChecksumCrc32C,
                    BytesPerChecksum = 64,
                },
                ChunkOffset = 0,
            },
        };
        response.WriteDelimitedTo(responseStream);
        var pktBytes = PacketBuilder.BuildCrc32C(0, 0, false, blockData, bpc: 64);
        responseStream.Write(pktBytes);
        var lastPkt = PacketBuilder.BuildEmptyLast(64, 1);
        responseStream.Write(lastPkt);
        responseStream.Position = 0;

        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(responseStream, writeStream, MockDataNode.DefaultDn);

        var reader = await RemoteBlockReader.CreateAsync(
            "/test/file", TestBlock, BlockToken.Empty,
            0, 64, true, "client", peer, peerCache: null, TestOptions);

        var buf = new byte[64];
        await ReadFullyAsync(reader, buf);
        await reader.DisposeAsync();

        // MemoryStream.ToArray() works even after disposal
        var writtenBytes = writeStream.ToArray();
        Assert.True(writtenBytes.Length > 3, "Expected client to write request + status");

        // The ClientReadStatusProto(CHECKSUM_OK) = varint(2) + tag(0x08) + value(0x06)
        // It's the last 3 bytes written after the OP_READ_BLOCK request.
        // Scan backwards for it.
        bool foundChecksumOk = false;
        for (int i = writtenBytes.Length - 3; i >= 0; i--)
        {
            try
            {
                var ms = new MemoryStream(writtenBytes, i, writtenBytes.Length - i);
                var status = Proto.ClientReadStatusProto.Parser.ParseDelimitedFrom(ms);
                if (status.Status == Proto.Status.ChecksumOk)
                {
                    foundChecksumOk = true;
                    break;
                }
            }
            catch
            {
                // Not a valid parse start, continue scanning
            }
        }

        Assert.True(foundChecksumOk, "Expected ClientReadStatusProto with CHECKSUM_OK");
    }

    [Fact]
    public async Task Dispose_CanBeCalledMultipleTimes()
    {
        var blockData = new byte[64];
        Random.Shared.NextBytes(blockData);

        var (peer, _) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 64);
        var reader = await CreateReader(peer, length: 64);

        var buf = new byte[64];
        await ReadFullyAsync(reader, buf);

        await reader.DisposeAsync();
        await reader.DisposeAsync(); // should not throw
    }

    [Fact]
    public async Task ReadAsync_AfterDispose_ThrowsObjectDisposed()
    {
        var blockData = new byte[64];
        Random.Shared.NextBytes(blockData);

        var (peer, _) = MockDataNode.CreateMockPeer(blockData, bytesPerChecksum: 64);
        var reader = await CreateReader(peer, length: 64);
        await reader.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await reader.ReadAsync(new byte[10]));
    }

    [Fact]
    public async Task PartialRead_RequestLessThanBlock()
    {
        // Block is 2048 bytes, read only 100
        var blockData = new byte[2048];
        Random.Shared.NextBytes(blockData);

        var (peer, expected) = MockDataNode.CreateMockPeer(
            blockData, bytesPerChecksum: 512, readLength: 100);
        await using var reader = await CreateReader(peer, length: 100);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(100, ms.Length);
        Assert.Equal(expected, ms.ToArray());
    }

    private static async Task ReadFullyAsync(Stream stream, byte[] buffer)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(total));
            if (read == 0) break;
            total += read;
        }
    }
}
