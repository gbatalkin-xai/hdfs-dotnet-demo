namespace Gtlm.Hdfs.Client.Net;

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

/// <summary>
/// Connection pool for DataNode peers, keyed by DataNode UUID.
/// After a clean block read, peers are returned here for reuse,
/// avoiding TCP handshake overhead on subsequent reads to the same DataNode.
/// </summary>
public sealed class PeerCache : IAsyncDisposable
{
    private readonly int _maxPerDataNode;
    private readonly TimeSpan _idleTimeout;
    private readonly ConcurrentDictionary<string, ConcurrentQueue<CachedPeer>> _cache = new();
    private readonly Timer _evictionTimer;
    private readonly ILogger<PeerCache>? _logger;
    private bool _disposed;

    public PeerCache(int maxPerDataNode, TimeSpan idleTimeout, ILogger<PeerCache>? logger = null)
    {
        _maxPerDataNode = maxPerDataNode;
        _idleTimeout = idleTimeout;
        _logger = logger;

        var evictionInterval = TimeSpan.FromMilliseconds(Math.Max(idleTimeout.TotalMilliseconds / 2, 1000));
        _evictionTimer = new Timer(EvictExpired, null, evictionInterval, evictionInterval);
    }

    /// <summary>
    /// Try to retrieve a cached peer for the given DataNode.
    /// Returns null if no valid cached connection exists.
    /// </summary>
    public Peer? TryGet(Models.DatanodeInfo dataNode)
    {
        if (_disposed) return null;

        if (!_cache.TryGetValue(dataNode.DatanodeUuid, out var queue))
            return null;

        while (queue.TryDequeue(out var cached))
        {
            if (cached.IsExpired(_idleTimeout) || cached.Peer.IsClosed)
            {
                _ = cached.Peer.DisposeAsync();
                continue;
            }

            _logger?.LogDebug("Reusing cached peer for {DataNode}", dataNode);
            return cached.Peer;
        }

        return null;
    }

    /// <summary>
    /// Return a peer to the cache for future reuse.
    /// Only call after a clean (no-error) block read completion.
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
            _ = peer.DisposeAsync();
            return;
        }

        queue.Enqueue(new CachedPeer(peer));
        _logger?.LogDebug("Returned peer to cache for {DataNode} (pool size: {Count})",
            peer.DataNode, queue.Count);
    }

    /// <summary>Number of cached peers across all DataNodes.</summary>
    internal int TotalCount
    {
        get
        {
            int count = 0;
            foreach (var (_, queue) in _cache)
                count += queue.Count;
            return count;
        }
    }

    private void EvictExpired(object? state)
    {
        if (_disposed) return;

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

            if (remaining.IsEmpty)
                _cache.TryRemove(uuid, out _);
            else
                _cache[uuid] = remaining;
        }
    }

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

    private sealed class CachedPeer
    {
        public Peer Peer { get; }
        private readonly long _cachedAtTicks;

        public CachedPeer(Peer peer)
        {
            Peer = peer;
            _cachedAtTicks = Environment.TickCount64;
        }

        public bool IsExpired(TimeSpan timeout) =>
            Environment.TickCount64 - _cachedAtTicks > (long)timeout.TotalMilliseconds;
    }
}
