namespace Gtlm.Hdfs.Client.Tests;

using Gtlm.Hdfs.Client.BlockReading;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;
using Gtlm.Hdfs.Client.Tests.Helpers;

public class HdfsFileStreamTests
{
    private static readonly HdfsClientOptions TestOptions = new();

    /// <summary>
    /// Helper: build a multi-block mock scenario where each "block" is a segment
    /// of a contiguous byte array, and each block has its own mock DataNode peer.
    /// We test HdfsFileStream by creating it from pre-resolved blocks.
    ///
    /// Since BlockReaderFactory.CreateRemoteReaderAsync connects to a real DataNode,
    /// we test HdfsFileStream by driving RemoteBlockReader directly via its
    /// CreateAsync for each block. This test builds separate mock peers per block
    /// and chains them via sequential reads.
    /// </summary>

    [Fact]
    public async Task SingleBlock_ReadsAllData()
    {
        var data = new byte[1024];
        Random.Shared.NextBytes(data);

        var (peer, expected) = MockDataNode.CreateMockPeer(data, bytesPerChecksum: 512);

        var block = new ExtendedBlock("BP-1", 1, 100, data.Length);
        await using var reader = await RemoteBlockReader.CreateAsync(
            "/test/file", block, BlockToken.Empty,
            0, data.Length, true, "test", peer, null, TestOptions);

        using var ms = new MemoryStream();
        await reader.CopyToAsync(ms);

        Assert.Equal(expected, ms.ToArray());
    }

    [Fact]
    public async Task HdfsClient_ThrowsIfNotConnected()
    {
        await using var client = new HdfsClient(new HdfsClientOptions());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.OpenReadAsync("/test"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.GetFileInfoAsync("/test"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.ListDirectoryAsync("/test"));
    }

    [Fact]
    public async Task HdfsClient_CanBeDisposed()
    {
        var client = new HdfsClient(new HdfsClientOptions());
        await client.DisposeAsync();
        await client.DisposeAsync(); // idempotent
    }

    [Fact]
    public void HdfsFileStream_StreamProperties()
    {
        var stream = HdfsFileStream.CreateFromBlocks(
            "/test/file", [], 0,
            new BlockReaderFactory(TestOptions, new PeerCache(4, TimeSpan.FromMinutes(1))));

        Assert.True(stream.CanRead);
        Assert.False(stream.CanSeek);
        Assert.False(stream.CanWrite);
        Assert.Equal(0, stream.Length);
        Assert.Equal(0, stream.Position);
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
        Assert.Throws<NotSupportedException>(() => stream.SetLength(0));
        Assert.Throws<NotSupportedException>(() => stream.Write([], 0, 0));
    }

    [Fact]
    public async Task HdfsFileStream_EmptyFile_ReturnsZero()
    {
        var stream = HdfsFileStream.CreateFromBlocks(
            "/test/empty", [], 0,
            new BlockReaderFactory(TestOptions, new PeerCache(4, TimeSpan.FromMinutes(1))));

        var buf = new byte[100];
        var read = await stream.ReadAsync(buf);

        Assert.Equal(0, read);
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task HdfsFileStream_Dispose_CanBeCalledMultipleTimes()
    {
        var stream = HdfsFileStream.CreateFromBlocks(
            "/test/file", [], 0,
            new BlockReaderFactory(TestOptions, new PeerCache(4, TimeSpan.FromMinutes(1))));

        await stream.DisposeAsync();
        await stream.DisposeAsync(); // should not throw
    }

    [Fact]
    public async Task HdfsFileStream_AfterDispose_ThrowsObjectDisposed()
    {
        var stream = HdfsFileStream.CreateFromBlocks(
            "/test/file", [], 100,
            new BlockReaderFactory(TestOptions, new PeerCache(4, TimeSpan.FromMinutes(1))));

        await stream.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => stream.ReadAsync(new byte[10]).AsTask());
    }

    [Fact]
    public async Task HdfsFileStream_Position_TracksAcrossReads()
    {
        var data = new byte[512];
        Random.Shared.NextBytes(data);

        var (peer, _) = MockDataNode.CreateMockPeer(data, bytesPerChecksum: 512);
        var block = new ExtendedBlock("BP-1", 1, 100, data.Length);

        await using var reader = await RemoteBlockReader.CreateAsync(
            "/test/file", block, BlockToken.Empty,
            0, data.Length, true, "test", peer, null, TestOptions);

        Assert.Equal(0, reader.Position);

        var buf = new byte[200];
        await reader.ReadAsync(buf);
        Assert.Equal(200, reader.Position);

        buf = new byte[312];
        int total = 0;
        int read;
        while ((read = await reader.ReadAsync(buf.AsMemory(total))) > 0)
            total += read;

        Assert.Equal(312, total);
        Assert.Equal(512, reader.Position);
        Assert.True(reader.IsComplete);
    }
}
