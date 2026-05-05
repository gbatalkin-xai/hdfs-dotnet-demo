namespace Gtlm.Hdfs.Client.Tests;

using Gtlm.Hdfs.Client.BlockReading;
using Gtlm.Hdfs.Client.Configuration;
using Gtlm.Hdfs.Client.Models;

public class LocalBlockReaderTests
{
    private static DatanodeInfo MakeDn(string ip = "10.0.0.1") => new()
    {
        IpAddress = ip,
        HostName = "dn1",
        DatanodeUuid = "u1",
        XferPort = 9866,
    };

    [Fact]
    public async Task TryCreateAsync_NoDomainSocketPath_ReturnsNull()
    {
        var options = new HdfsClientOptions { DomainSocketPath = null };
        var result = await LocalBlockReader.TryCreateAsync(
            new ExtendedBlock("BP-1", 1, 1), MakeDn(),
            0, 1024, true, options);

        Assert.Null(result);
    }

    [Fact]
    public async Task TryCreateAsync_RemoteDataNode_ReturnsNull()
    {
        var options = new HdfsClientOptions { DomainSocketPath = "/var/run/hdfs/dn" };
        var result = await LocalBlockReader.TryCreateAsync(
            new ExtendedBlock("BP-1", 1, 1), MakeDn("10.99.99.99"),
            0, 1024, true, options);

        Assert.Null(result);
    }

    [Fact]
    public async Task TryCreateAsync_LocalDataNode_ReturnsNull_NotYetImplemented()
    {
        // Even with localhost, full implementation is stubbed
        var options = new HdfsClientOptions { DomainSocketPath = "/var/run/hdfs/dn" };
        var result = await LocalBlockReader.TryCreateAsync(
            new ExtendedBlock("BP-1", 1, 1), MakeDn("127.0.0.1"),
            0, 1024, true, options);

        // Returns null because the FD-passing protocol is not yet implemented
        Assert.Null(result);
    }

    [Fact]
    public void IsLocalDataNode_Localhost_True()
    {
        Assert.True(LocalBlockReader.IsLocalDataNode(MakeDn("127.0.0.1")));
        Assert.True(LocalBlockReader.IsLocalDataNode(MakeDn("::1")));
        Assert.True(LocalBlockReader.IsLocalDataNode(MakeDn("localhost")));
    }

    [Fact]
    public void IsLocalDataNode_RemoteIp_False()
    {
        Assert.False(LocalBlockReader.IsLocalDataNode(MakeDn("10.99.99.99")));
    }

    [Fact]
    public async Task CreateFromStream_ReadsCorrectly()
    {
        var data = new byte[256];
        Random.Shared.NextBytes(data);
        var stream = new MemoryStream(data);

        await using var reader = LocalBlockReader.CreateFromStream(stream, data.Length);

        using var ms = new MemoryStream();
        await ((Stream)reader).CopyToAsync(ms);

        Assert.Equal(data, ms.ToArray());
    }

    [Fact]
    public async Task CreateFromStream_RespectsLength()
    {
        var data = new byte[1024];
        Random.Shared.NextBytes(data);
        var stream = new MemoryStream(data);

        // Only read 100 bytes even though stream has 1024
        await using var reader = LocalBlockReader.CreateFromStream(stream, 100);

        using var ms = new MemoryStream();
        await ((Stream)reader).CopyToAsync(ms);

        Assert.Equal(100, ms.Length);
        Assert.Equal(data[..100], ms.ToArray());
    }

    [Fact]
    public async Task CreateFromStream_Position_Tracks()
    {
        var data = new byte[100];
        var stream = new MemoryStream(data);
        await using var reader = LocalBlockReader.CreateFromStream(stream, 100);

        Assert.Equal(0, reader.Position);

        var buf = new byte[40];
        await reader.ReadAsync(buf);
        Assert.Equal(40, reader.Position);
        Assert.False(reader.IsComplete);

        buf = new byte[60];
        await reader.ReadAsync(buf);
        Assert.Equal(100, reader.Position);
        Assert.True(reader.IsComplete);
    }

    [Fact]
    public async Task CreateFromStream_StreamProperties()
    {
        var stream = new MemoryStream([1, 2, 3]);
        await using var reader = LocalBlockReader.CreateFromStream(stream, 3);

        Assert.True(reader.CanRead);
        Assert.False(reader.CanSeek);
        Assert.False(reader.CanWrite);
        Assert.Equal(3, reader.Length);
    }

    [Fact]
    public async Task Dispose_CanBeCalledMultipleTimes()
    {
        var stream = new MemoryStream([1]);
        var reader = LocalBlockReader.CreateFromStream(stream, 1);

        await reader.DisposeAsync();
        await reader.DisposeAsync(); // idempotent
    }

    [Fact]
    public async Task ReadAsync_AfterDispose_ThrowsObjectDisposed()
    {
        var stream = new MemoryStream([1]);
        var reader = LocalBlockReader.CreateFromStream(stream, 1);
        await reader.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => reader.ReadAsync(new byte[1]).AsTask());
    }
}
