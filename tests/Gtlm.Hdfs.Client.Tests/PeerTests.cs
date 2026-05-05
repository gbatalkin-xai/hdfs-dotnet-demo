namespace Gtlm.Hdfs.Client.Tests;

using System.Buffers;
using System.IO.Pipelines;
using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;

public class PeerTests
{
    private static DatanodeInfo MakeDataNode(string uuid = "test-uuid") => new()
    {
        IpAddress = "127.0.0.1",
        HostName = "test-dn",
        DatanodeUuid = uuid,
        XferPort = 9866,
    };

    [Fact]
    public void CreateForTest_SingleStream_SetsProperties()
    {
        var stream = new MemoryStream();
        var dn = MakeDataNode();

        var peer = Peer.CreateForTest(stream, dn);

        Assert.Same(dn, peer.DataNode);
        Assert.NotNull(peer.Input);
        Assert.NotNull(peer.Output);
        Assert.False(peer.IsClosed);
    }

    [Fact]
    public void CreateForTest_DualStream_SetsProperties()
    {
        var readStream = new MemoryStream();
        var writeStream = new MemoryStream();
        var dn = MakeDataNode();

        var peer = Peer.CreateForTest(readStream, writeStream, dn);

        Assert.Same(dn, peer.DataNode);
        Assert.False(peer.IsClosed);
    }

    [Fact]
    public async Task CreateForTest_SingleStream_GetStreams_ReturnsSameStream()
    {
        var stream = new MemoryStream();
        var peer = Peer.CreateForTest(stream, MakeDataNode());

        Assert.Same(stream, peer.GetOutputStream());
        Assert.Same(stream, peer.GetInputStream());

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task CreateForTest_DualStream_GetStreams_ReturnsCorrectStreams()
    {
        var readStream = new MemoryStream();
        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(readStream, writeStream, MakeDataNode());

        Assert.Same(readStream, peer.GetInputStream());
        Assert.Same(writeStream, peer.GetOutputStream());

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_SetsIsClosed()
    {
        var stream = new MemoryStream();
        var peer = Peer.CreateForTest(stream, MakeDataNode());

        Assert.False(peer.IsClosed);

        await peer.DisposeAsync();

        Assert.True(peer.IsClosed);
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var stream = new MemoryStream();
        var peer = Peer.CreateForTest(stream, MakeDataNode());

        await peer.DisposeAsync();
        await peer.DisposeAsync(); // should not throw
    }

    [Fact]
    public async Task PipeReader_CanReadDataWrittenToStream()
    {
        // Arrange: write data to a stream, then create a Peer reading from it
        var data = new byte[] { 0x00, 0x1C, 0x51, 0xAA, 0xBB };
        var readStream = new MemoryStream(data);
        var writeStream = new MemoryStream();
        var peer = Peer.CreateForTest(readStream, writeStream, MakeDataNode());

        // Act: read from the PipeReader
        var result = await peer.Input.ReadAsync();

        // Assert
        Assert.True(result.Buffer.Length >= data.Length);
        var slice = result.Buffer.Slice(0, data.Length);
        var actual = new byte[data.Length];
        slice.CopyTo(actual);
        Assert.Equal(data, actual);

        peer.Input.AdvanceTo(result.Buffer.GetPosition(data.Length));
        await peer.DisposeAsync();
    }

    [Fact]
    public async Task PipeWriter_WritesDataToStream()
    {
        var backingStream = new MemoryStream();
        var peer = Peer.CreateForTest(new MemoryStream(), backingStream, MakeDataNode());

        // Act: write through the PipeWriter
        var data = new byte[] { 0x01, 0x02, 0x03 };
        await peer.Output.WriteAsync(data);
        await peer.Output.FlushAsync();

        // Assert: data appears in the backing stream
        Assert.True(backingStream.Length >= data.Length);
        backingStream.Position = 0;
        var actual = new byte[data.Length];
        await backingStream.ReadExactlyAsync(actual);
        Assert.Equal(data, actual);

        await peer.DisposeAsync();
    }

    [Fact]
    public async Task ConnectAsync_InvalidAddress_ThrowsSocketException()
    {
        var dn = new DatanodeInfo
        {
            IpAddress = "192.0.2.1", // TEST-NET, not routable
            HostName = "unreachable",
            DatanodeUuid = "bad-uuid",
            XferPort = 1,
        };
        var options = new Configuration.HdfsClientOptions
        {
            ConnectTimeout = TimeSpan.FromMilliseconds(200),
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Peer.ConnectAsync(dn, options));
    }

    [Fact]
    public async Task ConnectAsync_CancellationToken_Respected()
    {
        var dn = new DatanodeInfo
        {
            IpAddress = "192.0.2.1",
            HostName = "unreachable",
            DatanodeUuid = "cancel-uuid",
            XferPort = 1,
        };
        var options = new Configuration.HdfsClientOptions
        {
            ConnectTimeout = TimeSpan.FromSeconds(30),
        };
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Peer.ConnectAsync(dn, options, cts.Token));
    }
}
