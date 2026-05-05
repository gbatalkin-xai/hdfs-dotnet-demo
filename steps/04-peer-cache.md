# Step 04: PeerCache (Connection Pool)

**Phase:** 1 (MVP)
**Prerequisites:** Step 03 (Peer)
**Produces:** `Net/PeerCache.cs` -- connection pool for DataNode socket reuse

---

## Objective

Implement a connection pool that caches idle `Peer` connections keyed by DataNode UUID.
After a block read completes cleanly, the peer is returned to the cache instead of being
closed. The next read to the same DataNode reuses the connection, avoiding TCP handshake
overhead.

This mirrors Java's `org.apache.hadoop.hdfs.net.PeerCache`.

---

## Tasks

### 4.1 `PeerCache` Class

**File:** `src/Gtlm.Hdfs.Client/Net/PeerCache.cs`

```csharp
namespace Gtlm.Hdfs.Client.Net;

using System.Collections.Concurrent;
using Gtlm.Hdfs.Client.Models;
using Microsoft.Extensions.Logging;

public sealed class PeerCache : IAsyncDisposable
{
    private readonly int _maxPerDataNode;
    private readonly TimeSpan _idleTimeout;
    private readonly ConcurrentDictionary<string, ConcurrentQueue<CachedPeer>> _cache = new();
    private readonly Timer _evictionTimer;
    private readonly ILogger<PeerCache> _logger;
    private bool _disposed;

    public PeerCache(int maxPerDataNode, TimeSpan idleTimeout, ILogger<PeerCache> logger)
    {
        _maxPerDataNode = maxPerDataNode;
        _idleTimeout = idleTimeout;
        _logger = logger;

        // Run eviction every idleTimeout/2
        var evictionInterval = TimeSpan.FromMilliseconds(idleTimeout.TotalMilliseconds / 2);
        _evictionTimer = new Timer(EvictExpired, null, evictionInterval, evictionInterval);
    }

    // ... method implementations below
}
```

### 4.2 `CachedPeer` -- Wrapper with Timestamp

```csharp
private sealed class CachedPeer
{
    public Peer Peer { get; }
    public long CachedAtTicks { get; }

    public CachedPeer(Peer peer)
    {
        Peer = peer;
        CachedAtTicks = Environment.TickCount64;
    }

    public bool IsExpired(TimeSpan timeout)
        => Environment.TickCount64 - CachedAtTicks > (long)timeout.TotalMilliseconds;
}
```

### 4.3 `TryGet` -- Retrieve a Cached Peer

```csharp
/// <summary>
/// Try to retrieve a cached peer for the given DataNode.
/// Returns null if no valid cached connection exists.
/// </summary>
public Peer? TryGet(DatanodeInfo dataNode)
{
    if (_disposed) return null;

    if (!_cache.TryGetValue(dataNode.DatanodeUuid, out var queue))
        return null;

    while (queue.TryDequeue(out var cached))
    {
        if (cached.IsExpired(_idleTimeout) || cached.Peer.IsClosed)
        {
            // Dispose expired/dead peers in background
            _ = cached.Peer.DisposeAsync();
            continue;
        }

        _logger.LogDebug("Reusing cached peer for {DataNode}", dataNode);
        return cached.Peer;
    }

    return null;
}
```

### 4.4 `Return` -- Return a Peer to the Cache

```csharp
/// <summary>
/// Return a peer to the cache for future reuse.
/// Only call this after a clean (no-error) block read completion.
/// The peer must have sent its final ClientReadStatusProto.
/// </summary>
public void Return(Peer peer)
{
    if (_disposed || peer.IsClosed)
    {
        _ = peer.DisposeAsync();
        return;
    }

    var queue = _cache.GetOrAdd(peer.DataNode.DatanodeUuid, _ => new ConcurrentQueue<CachedPeer>());

    if (queue.Count >= _maxPerDataNode)
    {
        // Pool full, dispose this peer
        _ = peer.DisposeAsync();
        return;
    }

    queue.Enqueue(new CachedPeer(peer));
    _logger.LogDebug("Returned peer to cache for {DataNode} (pool size: {Count})",
        peer.DataNode, queue.Count);
}
```

### 4.5 Eviction Timer

```csharp
private void EvictExpired(object? state)
{
    foreach (var (uuid, queue) in _cache)
    {
        var remaining = new ConcurrentQueue<CachedPeer>();
        while (queue.TryDequeue(out var cached))
        {
            if (cached.IsExpired(_idleTimeout) || cached.Peer.IsClosed)
            {
                _ = cached.Peer.DisposeAsync();
            }
            else
            {
                remaining.Enqueue(cached);
            }
        }

        // Swap the cleaned queue back (or remove if empty)
        if (remaining.IsEmpty)
            _cache.TryRemove(uuid, out _);
        else
            _cache[uuid] = remaining;
    }
}
```

### 4.6 `DisposeAsync` -- Drain All Connections

```csharp
public async ValueTask DisposeAsync()
{
    if (_disposed) return;
    _disposed = true;

    await _evictionTimer.DisposeAsync();

    foreach (var (_, queue) in _cache)
    {
        while (queue.TryDequeue(out var cached))
        {
            await cached.Peer.DisposeAsync();
        }
    }

    _cache.Clear();
}
```

---

## Design Decisions

- **`ConcurrentDictionary` + `ConcurrentQueue`:** Lock-free for the common case.
  Multiple block readers can return peers concurrently.
- **Keyed by `DatanodeUuid`:** UUIDs are globally unique. Avoids issues with IP
  changes or multiple ports on the same host.
- **Max per DataNode:** Prevents unbounded connection growth. Default 64 matches the
  Java client's typical configuration.
- **Eviction timer:** Proactively cleans up idle connections. Runs at half the idle
  timeout interval so connections don't linger much beyond their expiry.
- **No `TryGet` blocking/waiting:** If no cached peer exists, the caller creates a new
  connection. No contention on the pool.

---

## Acceptance Criteria

- [ ] `Return` → `TryGet` returns the same peer for the same DataNode
- [ ] `TryGet` returns `null` when no peers are cached
- [ ] Expired peers are not returned by `TryGet`
- [ ] Peers exceeding `maxPerDataNode` are disposed on `Return`
- [ ] Eviction timer disposes expired peers
- [ ] `DisposeAsync` disposes all cached peers
- [ ] Thread-safe: concurrent `Return` and `TryGet` calls don't corrupt state
