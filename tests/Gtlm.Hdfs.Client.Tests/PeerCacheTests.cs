namespace Gtlm.Hdfs.Client.Tests;

using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;

public class PeerCacheTests
{
    private static DatanodeInfo MakeDn(string uuid) => new()
    {
        IpAddress = "127.0.0.1",
        HostName = "dn-" + uuid,
        DatanodeUuid = uuid,
        XferPort = 9866,
    };

    private static Peer MakePeer(string uuid = "u1") =>
        Peer.CreateForTest(new MemoryStream(), MakeDn(uuid));

    [Fact]
    public void TryGet_Empty_ReturnsNull()
    {
        var cache = new PeerCache(maxPerDataNode: 4, idleTimeout: TimeSpan.FromMinutes(5));
        Assert.Null(cache.TryGet(MakeDn("u1")));
    }

    [Fact]
    public void Return_ThenTryGet_ReturnsSamePeer()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var peer = MakePeer("u1");

        cache.Return(peer);
        var got = cache.TryGet(MakeDn("u1"));

        Assert.Same(peer, got);
    }

    [Fact]
    public void TryGet_AfterRetrieved_ReturnsNull()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var peer = MakePeer("u1");

        cache.Return(peer);
        cache.TryGet(MakeDn("u1")); // consume it

        Assert.Null(cache.TryGet(MakeDn("u1")));
    }

    [Fact]
    public void TryGet_DifferentUuid_ReturnsNull()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        cache.Return(MakePeer("u1"));

        Assert.Null(cache.TryGet(MakeDn("u2")));
    }

    [Fact]
    public void Return_MultiplePeers_AllRetrievable()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var p1 = MakePeer("u1");
        var p2 = MakePeer("u1");

        cache.Return(p1);
        cache.Return(p2);

        var got1 = cache.TryGet(MakeDn("u1"));
        var got2 = cache.TryGet(MakeDn("u1"));

        Assert.NotNull(got1);
        Assert.NotNull(got2);
        Assert.Null(cache.TryGet(MakeDn("u1")));
    }

    [Fact]
    public async Task Return_ExceedsMax_DisposesExcess()
    {
        var cache = new PeerCache(maxPerDataNode: 2, idleTimeout: TimeSpan.FromMinutes(5));
        var p1 = MakePeer("u1");
        var p2 = MakePeer("u1");
        var p3 = MakePeer("u1");

        cache.Return(p1);
        cache.Return(p2);
        cache.Return(p3); // exceeds max of 2 — should be disposed

        // p3 was disposed, so only 2 in cache
        Assert.Equal(2, cache.TotalCount);

        Assert.True(p3.IsClosed);

        await cache.DisposeAsync();
    }

    [Fact]
    public async Task TryGet_ExpiredPeer_ReturnsNull()
    {
        // Use a very short timeout
        var cache = new PeerCache(4, idleTimeout: TimeSpan.FromMilliseconds(1));
        cache.Return(MakePeer("u1"));

        // Wait for expiry
        await Task.Delay(50);

        Assert.Null(cache.TryGet(MakeDn("u1")));

        await cache.DisposeAsync();
    }

    [Fact]
    public async Task TryGet_ClosedPeer_ReturnsNull()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var peer = MakePeer("u1");

        // Close the peer before returning
        await peer.DisposeAsync();
        cache.Return(peer);

        Assert.Null(cache.TryGet(MakeDn("u1")));
    }

    [Fact]
    public async Task Return_ClosedPeer_DisposesImmediately()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var peer = MakePeer("u1");
        await peer.DisposeAsync();

        cache.Return(peer); // should not add to cache

        Assert.Equal(0, cache.TotalCount);
    }

    [Fact]
    public async Task DisposeAsync_DisposesAllCachedPeers()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        var p1 = MakePeer("u1");
        var p2 = MakePeer("u2");

        cache.Return(p1);
        cache.Return(p2);

        await cache.DisposeAsync();

        Assert.True(p1.IsClosed);
        Assert.True(p2.IsClosed);
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        cache.Return(MakePeer("u1"));

        await cache.DisposeAsync();
        await cache.DisposeAsync(); // should not throw
    }

    [Fact]
    public async Task TryGet_AfterDispose_ReturnsNull()
    {
        var cache = new PeerCache(4, TimeSpan.FromMinutes(5));
        cache.Return(MakePeer("u1"));

        await cache.DisposeAsync();

        Assert.Null(cache.TryGet(MakeDn("u1")));
    }

    [Fact]
    public void ConcurrentAccess_ThreadSafe()
    {
        var cache = new PeerCache(100, TimeSpan.FromMinutes(5));

        // Many threads returning and getting peers concurrently
        Parallel.For(0, 100, i =>
        {
            var uuid = $"u{i % 10}";
            cache.Return(MakePeer(uuid));
            cache.TryGet(MakeDn(uuid));
        });

        // Should not throw or corrupt state
        Assert.True(cache.TotalCount >= 0);
    }
}
